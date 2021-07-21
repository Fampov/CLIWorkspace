# Databricks notebook source
# ingest raw data
import dbutils
from pyspark.shell import spark

blob_container = dbutils.widgets.get("blob_container")
patient_input_directory = dbutils.widgets.get("patient_input_directory")
source_patient_input_directory = dbutils.widgets.get("source_patient_input_directory")
patient_crosswalk_input_directory = dbutils.widgets.get("patient_crosswalk_input_directory")

blob_storage_account = "dosfhirstorage" # dbutils.widgets.get("blob_storage_account")
blob_access_token = "?sv=racwdl&sig=XPwqxHYf48VxjFaXiy26QhTerDObmxTJ6T21%3A47%3A16Z&se=2wbwmoUANW4%3D2019-10-10&st=2020-11-02021-11-03T20%3A47%3A00Z&sr=c&sp=" # dbutils.widgets.get("blob_access_token")
output_directory = "patientOutput" # dbutils.widgets.get("output_directory")
records_per_file = 100 # dbutils.widgets.get("records_per_file")

input_record_limit = 1000

file_root = "wasbs://" + blob_container + "@" + blob_storage_account + ".blob.core.windows.net"
spark.conf.set("fs.azure.sas." + blob_container + "." + blob_storage_account + ".blob.core.windows.net", blob_access_token)

patient_raw = spark.read \
  .options(delimiter='|', header="true").csv(file_root + "/" + patient_input_directory + "/").limit(input_record_limit) \
  .select("EDWPatientID", "MRN", "PatientFullNM", "GenderCD", "GenderDSC", "BirthDTS", "DeathDTS")
source_patient_raw = spark.read \
  .options(delimiter='|', header="true").csv(file_root + "/" + source_patient_input_directory + "/") \
  .select("PatientID", "RowSourceDSC", "PatientAddressLine01TXT", "PatientAddressLine02TXT", "PatientCityNM", "PatientStateCD", "PatientZipCD", "PrimaryPhoneNBR", "PatientEmailAddressTXT", "MRN", "PatientIDTypeDSC")
patient_crosswalk = spark.read \
  .options(delimiter='|', header="true").csv(file_root + "/" + patient_crosswalk_input_directory + "/") \
  .select("PatientID", "RowSourceDSC", "EDWPatientID")

source_patient = patient_crosswalk.join(source_patient_raw, ["PatientID", "RowSourceDSC"])

# COMMAND ----------

# build address from source patient
from pyspark.sql.functions import array, col, collect_list, expr, lit, struct, when

address_raw = source_patient \
  .withColumn("use", lit("home")) \
  .withColumn("type", lit("both")) \
  .withColumn("line", when(expr("PatientAddressLine02TXT IS NOT NULL"), array(col("PatientAddressLine01TXT"), col("PatientAddressLine02TXT"))).otherwise( array(col("PatientAddressLine01TXT")))) \
  .withColumn("text", col("PatientAddressLine01TXT") + " " + col("PatientAddressLine02TXT") + "," + col("PatientCityNM") + "," + col("PatientStateCD") + "," + col("PatientZipCD")) \
  .withColumnRenamed("PatientCityNM", "city") \
  .withColumn("district", lit("")) \
  .withColumnRenamed("PatientStateCD", "state") \
  .withColumnRenamed("PatientZipCD", "postalCode")

address = address_raw \
  .groupBy("EDWPatientID").agg(collect_list(struct("use", "type", "text", "line", "city", "district", "state", "postalCode")).alias("address"))

# COMMAND ----------

# build telecom from source patient
from pyspark.sql.functions import col, collect_list, expr, lit, struct

telephone = source_patient \
  .filter(expr("PrimaryPhoneNBR IS NOT NULL")) \
  .withColumn("use", lit("home")) \
  .withColumn("system", lit("phone")) \
  .withColumn("value", col("PrimaryPhoneNBR"))

email = source_patient \
  .filter(expr("PatientEmailAddressTXT IS NOT NULL")) \
  .withColumn("use", lit("home")) \
  .withColumn("system", lit("email")) \
  .withColumn("value", col("PatientEmailAddressTXT"))

telecom = telephone.unionAll(email) \
  .groupBy("EDWPatientID").agg(collect_list(struct("use", "system", "value")).alias("telecom"))

# COMMAND ----------

# build identifier from source patient
from pyspark.sql.functions import array, col, collect_list, expr, lit, struct

coding_lr = struct( \
  array(struct(lit("https://terminology.hl7.org/1.0.0/CodeSystem-v2-0203").alias("system"), lit("LR").alias("code"))).alias("coding"), \
  lit("Local Record ID").alias("text"))

coding_mr = struct( \
  array(struct(lit("https://terminology.hl7.org/1.0.0/CodeSystem-v2-0203").alias("system"), lit("MR").alias("code"))).alias("coding"), \
  lit("Medical Record").alias("text"))
              
id_edwpatient = source_patient \
  .filter(expr("EDWPatientID IS NOT NULL")) \
  .withColumn("use", lit("usual")) \
  .withColumn("type", coding_lr) \
  .withColumn("value", col("EDWPatientID")) \
  .withColumn("assigner", struct(lit("Health Catalyst").alias("display"))) \
  .select("EDWPatientID", "use", "type", "value", "assigner")

id_mrn = source_patient \
  .filter(expr("MRN IS NOT NULL")) \
  .withColumn("use", lit("usual")) \
  .withColumn("type", coding_mr) \
  .withColumn("value", col("MRN")) \
  .withColumn("assigner", struct(col("PatientIDTypeDSC").alias("display"))) \
  .select("EDWPatientID", "use", "type", "value", "assigner")

id_patient = source_patient \
  .filter(expr("PatientID IS NOT NULL")) \
  .withColumn("use", lit("usual")) \
  .withColumn("type", coding_lr) \
  .withColumn("value", col("PatientID")) \
  .withColumn("assigner", struct(col("PatientIDTypeDSC").alias("display"))) \
  .select("EDWPatientID", "use", "type", "value", "assigner")

identifier = id_edwpatient.unionAll(id_mrn).unionAll(id_patient) \
  .groupBy("EDWPatientID").agg(collect_list(struct("use", "type", "value", "assigner")).alias("identifier"))

# COMMAND ----------

# build patient
from pyspark.sql.functions import coalesce, col, concat, expr, lit, split, to_date, when

# work around an oddity with Spark 3 date parsing
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

patient = patient_raw \
  .join(address, ["EDWPatientID"]) \
  .join(telecom, ["EDWPatientID"]) \
  .join(identifier, ["EDWPatientID"]) \
  .withColumn("name", struct( \
    col("PatientFullNM").alias("text"), \
    split(col("PatientFullNM"), ", ")[0].alias("family"), \
    split(split(col("PatientFullNM"), ", ")[1], " ").alias("given"))) \
  .withColumn("resourceType", lit("Patient")) \
  .withColumnRenamed("EDWPatientID", "id") \
  .withColumn("active", lit("true")) \
  .withColumn("gender", when(col("GenderCD") == 1, "female").when(col("GenderCD") == 2, "male").otherwise("unknown")) \
  .withColumn("birthDate", to_date(col("BirthDTS"), "yyyy-MM-dd")) \
  .withColumn("deceasedBoolean", expr("DeathDTS IS NOT NULL")) \
  .withColumn("text", struct( \
    lit("generated").alias("status"), \
    concat(lit("<div xmlns='http://www.w3.org/1999/xhtml'><p><b>Generated Narrative</b></p><p><b>id</b>: "), coalesce(col("id"), lit("unknown")), lit("</p><p><b>identifier</b>: "), coalesce(col("MRN"), lit("unknown")), lit(" (MRN)</p><p><b>active</b>: true</p><p><b>name</b>: "), coalesce(col("PatientFullNM"), lit("unknown")), lit("</p><p><b>gender</b>: "), coalesce(col("GenderDSC"), lit("unknown")), lit("</p><p><b>birth date</b>: "), coalesce(col("birthDate"), lit("unknown")), lit("</p></div>")).alias("div"))) \
  .select("resourceType", "id", "active", "name", "identifier", "gender", "birthDate", "deceasedBoolean", "address", "telecom", "text")

# COMMAND ----------

# write results to blob
patient.write.mode("overwrite").option("maxRecordsPerFile", records_per_file).json(file_root + "/" + output_directory + "/")