SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    FacilityID,
    Emr,
    Project,
    VisitID,
    VisitDate,
    CASE
        WHEN PHQ9_1 = 0 THEN 'Not at all'
        WHEN PHQ9_1 = 1 THEN 'Several days'
        WHEN PHQ9_1 = 2 THEN 'More than half'
        WHEN PHQ9_1 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-1',
    CASE
        WHEN PHQ9_2 = 0 THEN 'Not at all'
        WHEN PHQ9_2 = 1 THEN 'Several days'
        WHEN PHQ9_2 = 2 THEN 'More than half'
        WHEN PHQ9_3 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-2',
    CASE
        WHEN PHQ9_3 = 0 THEN 'Not at all'
        WHEN PHQ9_3 = 1 THEN 'Several days'
        WHEN PHQ9_3 = 2 THEN 'More than half'
        WHEN PHQ9_3 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-3',
    CASE
        WHEN PHQ9_4 = 0 THEN 'Not at all'
        WHEN PHQ9_4 = 1 THEN 'Several days'
        WHEN PHQ9_4 = 2 THEN 'More than half'
        WHEN PHQ9_4 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-4',
    CASE
        WHEN PHQ9_5 = 0 THEN 'Not at all'
        WHEN PHQ9_5 = 1 THEN 'Several days'
        WHEN PHQ9_5 = 2 THEN 'More than half'
        WHEN PHQ9_5 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-5',
    CASE
        WHEN PHQ9_6 = 0 THEN 'Not at all'
        WHEN PHQ9_6 = 1 THEN 'Several days'
        WHEN PHQ9_6 = 2 THEN 'More than half'
        WHEN PHQ9_6 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-6',
    CASE
        WHEN PHQ9_7 = 0 THEN 'Not at all'
        WHEN PHQ9_7 = 1 THEN 'Several days'
        WHEN PHQ9_7 = 2 THEN 'More than half'
        WHEN PHQ9_7 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-7',
    CASE
        WHEN PHQ9_8 = 0 THEN 'Not at all'
        WHEN PHQ9_8 = 1 THEN 'Several days'
        WHEN PHQ9_8 = 2 THEN 'More than half'
        WHEN PHQ9_8 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-8',
    CASE
        WHEN PHQ9_9 = 0 THEN 'Not at all'
        WHEN PHQ9_9 = 1 THEN 'Several days'
        WHEN PHQ9_9 = 2 THEN 'More than half'
        WHEN PHQ9_9 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-9',
    CASE
        WHEN PHQ9Score >= 0 AND PHQ9Score <= 4 THEN 'Depression unlikely'
        WHEN PHQ9Score >= 5 AND PHQ9Score <= 9 THEN 'Mild depression'
        WHEN PHQ9Score >= 10 AND PHQ9Score <= 14 THEN 'Moderate depression'
        WHEN PHQ9Score >= 15 AND PHQ9Score <= 19 THEN 'Moderate severe depression'
        WHEN PHQ9Score >= 20 AND PHQ9Score <= 27 THEN 'Severe depression'
        ELSE NULL
    END AS 'PHQ9Score',
    PHQ9Rating AS 'PHQ9 Rating'
FROM
    ndwr.ndwr_patient_depression_screening;