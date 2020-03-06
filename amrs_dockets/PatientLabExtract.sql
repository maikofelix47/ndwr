use ndwr;
select 
PatientPK,
PatientID,
FacilityID,
SiteCode,
Emr,
Project,
FacilityName,
SatelliteName,
VisitID,
OrderedbyDate,
ReportedbyDate,
TestName,
EnrollmentTest,
TestResult,
Reason from ndwr.ndwr_patient_labs_extract;