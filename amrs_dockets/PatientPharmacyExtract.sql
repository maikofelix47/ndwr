use ndwr;
SELECT 
    PatientPK,
    PatientID,
    FacilityID,
    SiteCode,
    Emr,
    Project,
    VisitID,
    Drug,
    Provider,
    DispenseDate,
    Duration,
    IF(ExpectedReturn IS NULL,date_add(DispenseDate,interval 21 day),ExpectedReturn) AS ExpectedReturn,
    TreatmentType,
    RegimenLine,
    PeriodTaken,
    ProphylaxisType
FROM
    ndwr.ndwr_patient_pharmacy;