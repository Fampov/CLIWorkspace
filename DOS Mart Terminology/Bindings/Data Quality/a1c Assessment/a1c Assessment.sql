SELECT WI.contributorid AS ContributorID
	,WI.contributoritemid AS ContributorAssessmentID
	,wi.subjecttxt AS SubjectTXT
	,wi.nametxt AS NameTXT
	,wi.descriptiontxt AS DescriptionTXT
	,wi.groupname AS GroupNM
	,case when wi.valuenbr > 2 then 'fail' else 'pass' end AS CategoryCD
	,'Hemoglobin A1c should not go above 9.0' AS AssertionTXT
	,'Intervine with patient as soon as possible' AS CommentTXT
	,wi.sourcedataasofdts AS SourceDataAsOfDTS
	,wi.valuenbr AS ValueNBR
	,0 AS LowerLimitNBR
	,2 AS UpperLimitNBR
	,wi.uniqueitemid AS UniqueItemID
	,1 AS ActiveBIT
	,1 AS PublicBIT
FROM [dataquality].[core].[watchitem] as WI union select top 0 * from dataquality.staging.watchitemue