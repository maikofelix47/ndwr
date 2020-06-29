CALL `ndwr`.`build_NDWR_all_patients_extract`("build",1, 10, 1,'2020-05-31',true);
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
select distinct PatientID from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_all_patient_status_extract`("build",1,1,1,true);
###################################
replace into ndwr.ndwr_pharmacy_build_queue (
select distinct person_id from etl.flat_hiv_summary_v15b where cur_arv_meds is not null
);
##################################
replace into ndwr.ndwr_patient_labs_extract_build_queue (
select distinct PatientID from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_ndwr_patient_labs_extract`("build",1,1,1,true);