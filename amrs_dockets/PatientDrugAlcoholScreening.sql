SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    DrinkAlcohol AS 'TEST',
    CASE
        WHEN DrinkAlcohol = 1090  THEN 'Never'
        WHEN DrinkAlcohol = 1091  THEN 'Monthly or less'
        WHEN DrinkAlcohol = 1092  THEN '2 to 4 times a month'
        WHEN DrinkAlcohol = 1093  THEN '2 to 3 times a week'
        WHEN DrinkAlcohol IN (1094,1095)  THEN '4 or More Times a Week'
        ELSE NULL
    END AS 'DrinkAlcohol',
    NULL AS 'Smoking',
    NULL AS 'DrugUse'
FROM
    ndwr.ndwr_drug_alcohol_screening 