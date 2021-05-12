SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    SessionNumber,
    FirstSessionDate,
    PillCountAdherence,
    CASE
     WHEN MMAS4_1 = 1 THEN 'YES'
     WHEN MMAS4_1 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4-1',
    CASE
     WHEN MMAS4_2 = 1 THEN 'YES'
     WHEN MMAS4_2 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4-2',
	CASE
     WHEN MMAS4_3 = 1 THEN 'YES'
     WHEN MMAS4_3 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4-3',
    CASE
     WHEN MMAS4_4 = 1 THEN 'YES'
     WHEN MMAS4_4 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4_4',
	CASE
     WHEN MMAS4_4 = 1 THEN 'YES'
     WHEN MMAS4_4 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4_4',
    CASE
     WHEN MMSA8_1 = 1 THEN 'YES'
     WHEN MMSA8_1 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMSA8_1',
    CASE
     WHEN MMSA4_2 = 1 THEN 'YES'
     WHEN MMSA4_2 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMSA4_2',
    CASE
     WHEN MMSA4_3 = 1 THEN 'YES'
     WHEN MMSA4_3 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMSA4_3',
    CASE
     WHEN MMSA4_4 = 0 THEN 'Never/Rarely'
     WHEN MMSA4_4 = 0.25 THEN 'Once in a while'
     WHEN MMSA4_4 = 0.5 THEN 'Sometimes'
     WHEN MMSA4_4 = 0.75 THEN 'Usually'
     WHEN MMSA4_4 = 1 THEN 'All the time'
     ELSE NULL
    END AS 'MMSA4_4',
    CASE
     WHEN MMSAScore >= 0 AND MMSAScore <= 0.25 THEN 'Good'
     WHEN MMSAScore >= 0.5 AND MMSAScore <= 2 THEN 'Inadequate'
     WHEN MMSAScore >= 3 AND MMSAScore <= 8 THEN 'Poor'
     ELSE 'Unknown'
    END AS 'MMSAScore',
    EACRecievedVL,
    EACVL,
    EACVLConcerns,
    EACVLThoughts,
    EACWayForward,
    EACCognitiveBarrier,
    EACBehaviouralBarrier_1,
    EACBehaviouralBarrier_2,
    EACBehaviouralBarrier_3,
    EACBehaviouralBarrier_4,
    EACBehaviouralBarrier_5,
    EACEmotionalBarriers_1,
    EACEmotionalBarriers_2,
    EACEconBarrier_1,
    EACEconBarrier_2,
    EACEconBarrier_3,
    EACEconBarrier_4,
    EACEconBarrier_5,
    EACEconBarrier_6,
    EACEconBarrier_7,
    EACEconBarrier_8,
    EACReviewImprovement,
    EACReviewMissedDoses,
    EACReviewStrategy,
    EACReferral,
    EACReferralApp,
    EACReferralExperience,
    EACHomevisit,
    EACAdherencePlan,
    EACFollowupDate,
    DateCreated
FROM
    ndwr.ndwr_patient_eac
ORDER BY VisitDate;