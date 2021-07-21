SELECT 3 AS ContributorID
	,9000 AS ContributorItemID
	,'databaseentity.[Patient].[Diagnosis].[Patient]' AS SubjectTXT
	,'Hemoglobin A1c test out of control > 9.0' AS NameTXT
	,'Flags patients whose Hemoglobin A1c is greater than 9.0' AS DescriptionTXT
	,'Hemoglobin A1c' AS GroupNM
	, max(LastLoadDTS) AS SourceDataAsOfDTS
	,CAST(sum(case when diagnosisCD is null then 1 else 0 end) AS VARCHAR(255))AS ValueTXT
	,1 AS ActiveBIT
	,1 AS PublicBIT
FROM [Patient].[Diagnosis].[Patient]


SELECT vsc.CodeCD, vsc.CodeDSC, vsc.CodeSystemNM, vsd.ValueSetNM,
            vsd.DefinitionDSC, vsd.AuthorityDSC, vsd.SourceDSC, vsd.ValueSetReferenceID
        FROM Shared.Terminology.ValueSetDescription vsd
        INNER JOIN Shared.Terminology.ValueSetCode vsc ON vsc.ValueSetGUID = vsd.ValueSetGUID
        WHERE vsd.ValueSetReferenceID IN ('f1918fdf-61fa-451a-a1a7-4d60ef933a01')
        AND vsd.StatusCD = 'Active' AND vsd.LatestVersionFLG = 'Y'
        ;