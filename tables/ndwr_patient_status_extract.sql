CREATE TABLE `ndwr`.`ndwr_patient_status_extract` (
  `PatientPK` INT NOT NULL,
  `PatientID` INT NOT NULL,
  `FacilityId` INT NOT NULL,
  `SiteCode` VARCHAR(50) NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `FacilityName` VARCHAR(50) NOT NULL,
  `ExitDescription` VARCHAR(50) NULL,
  `ExitDate` DATETIME NULL,
  `ExitReason` VARCHAR(200) NULL);