SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    FacilityID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    CASE
      WHEN OnTBDrugs = 1 THEN 'YES'
      WHEN OnTBDrugs = 0 THEN 'NO'
      ELSE NULL
    END AS 'OnTBDrugs',
    CASE
      WHEN OnIPT = 1 THEN 'YES'
      WHEN OnIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'OnIPT',
    CASE
      WHEN EverOnIPT = 1 THEN 'YES'
      WHEN EverOnIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'EverOnIPT',
    CASE
      WHEN Cough = 1 THEN 'YES'
      WHEN Cough = 0 THEN 'NO'
      ELSE NULL
    END AS 'Cough',
    CASE
      WHEN Fever = 1 THEN 'YES'
      WHEN Fever = 0 THEN 'NO'
      ELSE NULL
    END AS 'Fever',
    CASE
      WHEN NoticeableWeightLoss = 1 THEN 'YES'
      WHEN NoticeableWeightLoss = 0 THEN 'NO'
      ELSE NULL
    END AS 'NoticeableWeightLoss',
	CASE
      WHEN NightSweats = 1 THEN 'YES'
      WHEN NightSweats = 0 THEN 'NO'
      ELSE NULL
    END AS 'NightSweats',
    CASE
      WHEN  Lethargy = 1 THEN 'YES'
      WHEN  Lethargy = 0 THEN 'NO'
      ELSE NULL
    END AS 'Lethargy',
    ICFActionTaken,
    TestResult,
    TBClinicalDiagnosis,
    ContactsInvited,
    CASE
      WHEN  EvaluatedForIPT = 1 THEN 'YES'
      WHEN  EvaluatedForIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'EvaluatedForIPT',
    CASE
      WHEN  StartAntiTBs = 1 THEN 'YES'
      WHEN  StartAntiTBs = 0 THEN 'NO'
      ELSE NULL
    END AS 'StartAntiTBs',
    TBRxStartDate,
    TBScreening,
    IPTClientWorkUp,
    CASE
      WHEN  StartIPT = 1 THEN 'YES'
      WHEN  StartIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'StartIPT',
    IndicationForIPT
FROM
    ndwr.ndwr_patient_ipt_extract;