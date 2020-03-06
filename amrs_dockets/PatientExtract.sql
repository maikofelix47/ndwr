use ndwr;
SELECT 
    PatientPK,
    PatientID,
    FacilityID,
    SiteCode,
    Emr,
    Project,
    FacilityName,
    Gender,
    DOB,
    CASE
      WHEN IF(RegistrationDate < '1997-01-01','1997-01-01',RegistrationDate) < DOB THEN DATE_ADD(DOB, INTERVAL 10 DAY)
      ELSE RegistrationDate
    END AS `RegistrationDate`,
    IF(RegistrationDate < '1997-01-01','1997-01-01',RegistrationDate) AS `RegistrationDate`,
    IF(RegistrationAtCCC < '1997-01-01','1997-01-01', RegistrationAtCCC) AS  `RegistrationAtCCC`,
    RegistrationAtPMTCT,
    RegistrationAtTBClinic,
    PatientSource,
    Region,
    District,
    Village,
    ContactRelation,
    LastVisit,
    MaritalStatus,
    EducationLevel,
    CASE
       WHEN DateConfirmedHIVPositive IS NOT NULL THEN IF(DateConfirmedHIVPositive < '1997-01-01','1997-01-01', DateConfirmedHIVPositive)
       ELSE DateConfirmedHIVPositive
    END AS `DateConfirmedHIVPositive`,
    PreviousARTExposure,
    PreviousARTStartDate,
    StatusAtCCC,
    StatusAtPMTCT,
    StatusAtTBClinic,
    SatelliteName,
    Inschool,
    KeyPopulationType,
    Orphan,
    PatientResidentCounty,
    PatientResidentLocation,
    PatientResidentSubCounty,
    PatientResidentSubLocation,
    PatientResidentVillage,
    PatientResidentWard,
    PatientType,
    PopulationType,
    TransferInDate
FROM
    ndwr.ndwr_all_patients_extract