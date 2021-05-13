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
    ChronicIllness,
    ChronicOnsetDate,
    CASE
        WHEN knownAllergies = 1 THEN 'YES'
        WHEN knownAllergies = 0 THEN 'NO'
        ELSE NULL
    END AS 'knownAllergies',
    AllergyCausativeAgent,
    AllergicReaction,
    AllergySeverity,
    AllergyOnsetDate,
    Skin,
    Eyes,
    ENT,
    Chest,
    CVS,
    Abdomen,
    CNS,
    Genitourinary
FROM
    ndwr.ndwr_patient_allergies_chronic_illness;