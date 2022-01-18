CREATE TABLE IF NOT EXISTS `ndwr_all_patient_status_extract` (
    `PatientPK` INT NOT NULL,
    `PatientID` INT NOT NULL,
    `FacilityId` INT NOT NULL,
    `SiteCode` VARCHAR(50) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(50) NOT NULL,
    `ExitDescription` VARCHAR(50) NULL,
    `ExitDate` DATETIME NULL,
    `ExitReason` VARCHAR(200) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     INDEX status_patient_id (PatientID),
     INDEX status_patient_pk (PatientPK),
     INDEX status_facility_id (FacilityID),
     INDEX status_site_code (SiteCode),
     INDEX status_date_created (DateCreated),
     INDEX status_patient_facility (PatientID,FacilityID)
);