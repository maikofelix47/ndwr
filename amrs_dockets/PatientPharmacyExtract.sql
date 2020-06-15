SELECT 
    PatientPK,
    PatientID,
    FacilityID,
    SiteCode,
    Emr,
    Project,
    VisitID,
    REPLACE(Drug,'##','+') as `Drug`,
    Provider,
    DispenseDate,
    Duration,
    IF(ExpectedReturn IS NULL || DATE(ExpectedReturn) = '0000-00-00',date_add(DispenseDate,interval 21 day),ExpectedReturn) AS ExpectedReturn,
    TreatmentType,
    RegimenLine,
    PeriodTaken,
    ProphylaxisType
FROM
    ndwr.ndwr_patient_pharmacy;