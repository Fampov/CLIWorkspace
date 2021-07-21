WITH RawICDAttributes as (
select 
    distinct CodeGUID, 
             AttributeNM,
    (select distinct 
	
	CASE WHEN AttributeTypeCD = 'string' then [AttributeValueTXT]
	WHEN AttributeTypeCD = 'number' then CAST([AttributeValueNBR] as varchar)
	WHEN AttributeTypeCD = 'longstring' then CAST(AttributeValueLongTXT as varchar)
	WHEN AttributeTypeCD = 'date' then CAST(AttributeValueDTS as varchar)
	END as AttributeValue
  
  from [Shared].[Terminology].[Attribute] t2  where t1.CodeGUID=t2.CodeGUID and t2.AttributeNM=t1.AttributeNM) as AttributeValue

from [Shared].[Terminology].[Attribute] t1 
where t1.codeguid in
  (SELECT [CodeGUID] FROM [Shared].[Terminology].[Code] where CodeSystemGUID in ('87f53b39-2edf-4045-82cf-93010055a5b8', '87846e70-ca84-4d5d-b414-c301f7bfefaa'))
  )
  
,RawTerms as (
SELECT 
       [CodeGUID]
      ,[TermTXT]
      ,[TermTypeCD]
  FROM [Shared].[Terminology].[Term]
  WHERE [CodeGUID] in
  (SELECT [CodeGUID] FROM [Shared].[Terminology].[Code]
    where CodeSystemGUID in ('87f53b39-2edf-4045-82cf-93010055a5b8', '87846e70-ca84-4d5d-b414-c301f7bfefaa'))
   )

,TermPivot as (

select CodeGUID, ICD10Formatted, ICD10Raw, ICD9CMFormatted, ICD9CMRaw
from (
    select *, rowno = row_number() OVER (PARTITION BY CodeGUID, [TermTypeCD] Order By CodeGUID, [TermTypeCD])
    from
        RawTerms t
    ) unpvt
    PIVOT (min([TermTXT]) FOR [TermTypeCD] in (ICD10Formatted, ICD10Raw, ICD9CMFormatted, ICD9CMRaw)) pvt
	)

, ICDAttributeInfo as (
select CodeGUID, HeaderIndicatorFLG
from (
    select *, rowno = row_number() OVER (PARTITION BY CodeGUID, AttributeNM Order By CodeGUID, AttributeNM)
    from
        RawICDAttributes t
    ) unpvt
    PIVOT (min(AttributeValue) FOR AttributeNM in (HeaderIndicatorFLG, ICD10Formatted, ICD10Raw, ICD9CMFormatted, ICD9CMRaw)) pvt)
	
SELECT 

       a.[CodeCD] as DiagnosisCD
	   ,coalesce(d.ICD10Raw, d.ICD9CMRaw) as UnformattedDiagnosisCD
	  ,CASE WHEN a.CodeSystemGUID = '87f53b39-2edf-4045-82cf-93010055a5b8' then 'ICD10DX'
	       WHEN a.CodeSystemGUID = '87846e70-ca84-4d5d-b414-c301f7bfefaa' then 'ICD9DX'
		   ELSE NULL END as [CodeTypeCD]
	  ,CASE WHEN a.CodeSystemGUID = '87f53b39-2edf-4045-82cf-93010055a5b8' then '10'
	       WHEN a.CodeSystemGUID = '87846e70-ca84-4d5d-b414-c301f7bfefaa' then '9'
		   ELSE NULL END as [ICDRevisionCD]
      ,a.[CodeDSC] as DiagnosisDSC

  FROM [Shared].[Terminology].[Code] a
  left outer join 
  ICDAttributeInfo b
  on (a.CodeGUID = b.CodeGUID)
  left outer join
  TermPivot d
  on (a.CodeGUID = d.CodeGUID)
  left outer join
  [Shared].[Terminology].CodeSystem c
  on (a.CodeSystemGUID = c.CodeSystemGUID)
where a.CodeSystemGUID in  ('87f53b39-2edf-4045-82cf-93010055a5b8'/*10*/, '87846e70-ca84-4d5d-b414-c301f7bfefaa'/*9*/)