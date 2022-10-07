DELIMITER $$
CREATE  PROCEDURE `ndwr_delete_innvalid_records`()
BEGIN
SELECT CONCAT('Deleting invalid records...');

SELECT CONCAT('Deleting invalid visit records...');
delete from ndwr.ndwr_all_patient_visits_extract where VisitID in (1516465,1582712,2672191,2991946,208124,2966113,2526641,2007432,5481734,5335355,5339572,5339804,5337479,5338987,5344988,5338989);

SELECT CONCAT('Deleting invalid all patients records where RegistrationDate = "0000-00-00 00:00:00"');
delete from ndwr.ndwr_all_patients_extract where RegistrationDate = '0000-00-00 00:00:00';

SELECT CONCAT('Deleting invalid ART records where RegistrationDate = "0000-00-00 00:00:00"');
delete from ndwr.ndwr_patient_art_extract where RegistrationDate = '0000-00-00 00:00:00';

SELECT CONCAT('Deleting invalid pharmacy records where RegistrationDate = "0000-00-00 00:00:00"');
delete from ndwr.ndwr_pharmacy where VisitID in (3739254,5283474,3529056,3575124,4752219,5338987,5337479,5339804,5339572,5335355,5481734,5344988,5338989,1516465,2672191,2991946,2526641,2007432);

SELECT CONCAT('Deleting invalid Defaulter Tracing records where BookingDate = "0000-00-00 00:00:00"');
delete from  ndwr.ndwr_defaulter_tracing_extract where BookingDate = '0000-00-00 00:00:00';
END$$
DELIMITER ;
