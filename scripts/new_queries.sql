replace into ndwr.ndwr_all_patients_build_queue(
select distinct person_id from etl.flat_hiv_summary_v15b where is_clinical_encounter = 1
        AND next_clinical_datetime_hiv IS NULL
);
CALL `ndwr`.`build_NDWR_all_patients_extract`("build",1, 10, 1,"true");
#################################
replace into ndwr.ndwr_patient_adverse_events_build_queue (
select distinct PatientID from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_adverse_event`("build",1,1,1,true);
##################################
replace into ndwr.ndwr_patient_art_extract_build_queue (
select distinct PatientID from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_patient_art_extract`("build",1,1,1,true);
##################################
replace into ndwr.ndwr_all_patient_status_extract_build_queue (
select distinct PatientID from ndwr.ndwr_all_patients_extract where StatusAtCCC in('dead','ltfu','transfer_out')
);
CALL `ndwr`.`build_NDWR_all_patient_status_extract`("build",1,1,1,true);
###################################
replace into ndwr.ndwr_pharmacy_build_queue (
select distinct person_id from etl.flat_hiv_summary_v15b where cur_arv_meds is not null and encounter_datetime >= '2021-03-01 00:00:00'
);
CALL `ndwr`.`build_NDWR_pharmacy`("build",1,1,1,true);
##################################
replace into ndwr.ndwr_patient_labs_extract_build_queue (
select distinct PatientID from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_ndwr_patient_labs_extract`("build",1,1,1,true);
###################################
replace into ndwr.ndwr_all_patient_visits_extract_build_queue (
select distinct PatientID from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_all_patient_visits_extract`("build",1,1,1,true);
##################################
use ndwr;
replace into ndwr.ndwr_patient_baselines_extract_build_queue (
   select distinct s.person_id from etl.flat_hiv_summary_v15b s where	(s.arv_first_regimen_start_date <> ''	or s.enrollment_date<>'') and encounter_datetime >= '2021-03-01 00:00:00'
);
CALL `ndwr`.`build_ndwr_patient_baselines_extract`("build",1,1,true);