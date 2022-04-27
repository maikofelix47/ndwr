use ndwr;
CREATE TABLE ndwr.ndwr_defaulter_tracing_extract(
   `PatientPK` INT NOT NULL,
   `SiteCode` VARCHAR(45) NOT NULL,
   `PatientID` INT NOT NULL,
   `Emr` VARCHAR(50) NULL,
   `Project` VARCHAR(50) NULL,
   `FacilityName` VARCHAR(100) NULL,
   `FacilityId` INT NULL,
   `VisitID` INT NULL,
   `VisitDate` DATETIME NULL,
   `EncounterId` INT NULL,
    `TracingType` VARCHAR(50) NULL,
    `TracingOutcome` VARCHAR(50) NULL,
    `AttemptNumber` INT NOT NULL,
    `IsFinalTrace` VARCHAR(50) NULL,
    `TrueStatus` VARCHAR(50) NULL,
    `CauseOfDeath` VARCHAR(50) NULL,
    `Comments` VARCHAR(50) NULL,
    `BookingDate` VARCHAR(50) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     INDEX dt_patient_id (PatientID),
	   INDEX visit_site_code (SiteCode)
    );