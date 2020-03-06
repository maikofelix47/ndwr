use ndwr;
select 
PatientPK,
PatientID,
FacilityID,
SiteCode,
Emr,
Project,
FacilityName,
ExitDescription,
ExitDate,
ExitReason from ndwr.ndwr_patient_status_extract;