CREATE TABLE `ndwr`.`ndwr_patient_labs_extract` (
  `PatientPK` INT NOT NULL,
  `PatientID` INT NOT NULL,
  `FacilityID` INT NULL,
  `SiteCode` INT NULL,
  `Emr` VARCHAR(50) NOT NULL,
  `Project` VARCHAR(50) NOT NULL,
  `FacilityName` VARCHAR(100) NOT NULL,
  `SatelliteName` VARCHAR(50) NULL,
  `VisitID` INT NULL,
  `OrderedbyDate` DATETIME NOT NULL,
  `ReportedbyDate` DATETIME NOT NULL,
  `TestName` VARCHAR(200) NULL,
  `EnrollmentTest` VARCHAR(50) NULL,
  `TestResult` INT NOT NULL,
  `Reason` VARCHAR(200) NULL
  );