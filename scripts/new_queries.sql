replace into ndwr.ndwr_all_patients_build_queue(
select distinct person_id from etl.flat_hiv_summary_v15b where is_clinical_encounter = 1
        AND next_clinical_datetime_hiv IS NULL
);
CALL `ndwr`.`build_NDWR_all_patients_extract`("build",1, 10, 1,"true");
#################################
replace into ndwr.ndwr_patient_adverse_events_build_queue (
select distinct PatientPK from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_adverse_event`("build",1,1,1,true);
##################################
replace into ndwr.ndwr_patient_art_extract_build_queue (
select distinct PatientPK from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_patient_art_extract`("build",1,1,1,true);
##################################
replace into ndwr.ndwr_all_patient_status_extract_build_queue (
select distinct PatientPK from ndwr.ndwr_all_patients_extract where StatusAtCCC in('dead','ltfu','transfer_out') limit 5
);
CALL `ndwr`.`build_NDWR_all_patient_status_extract`("build",1,1,1,true);
###################################
replace into ndwr.ndwr_pharmacy_build_queue (
select distinct person_id from etl.flat_hiv_summary_v15b where cur_arv_meds is not null and encounter_datetime >= '2021-03-01 00:00:00'
);
CALL `ndwr`.`build_NDWR_pharmacy`("build",1,1,1,true);
##################################
replace into ndwr.ndwr_patient_labs_extract_build_queue(
select distinct person_id from amrs.obs where concept_id in (856,730,5497)
    and voided = 0 and date_created >= '2021-01-01'
);
CALL `ndwr`.`build_NDWR_ndwr_patient_labs_extract`("build",1,1,1,true);
###################################
replace into ndwr.ndwr_all_patient_visits_extract_build_queue (
select distinct PatientPK from ndwr.ndwr_all_patients_extract
);
CALL `ndwr`.`build_NDWR_all_patient_visits_extract`("build",1,1,1,true);
##################################
use ndwr;
replace into ndwr.ndwr_patient_baselines_extract_build_queue (
   select distinct s.person_id from etl.flat_hiv_summary_v15b s where	(s.arv_first_regimen_start_date <> ''	or s.enrollment_date<>'') and encounter_datetime >= '2021-03-01 00:00:00'
);
CALL `ndwr`.`build_ndwr_patient_baselines_extract`("build",1,1,true);
##############################################
replace into ndwr.ndwr_patient_depression_screening_build_queue(
select distinct 
o.person_id from amrs.obs o where
 o.concept_id in(7806,7807,7808,7809,7810,7811,7812,7813,7814)
 and o.voided = 0 and date_created >= '2021-01-01 00:00:00'
);
CALL `ndwr`.`build_ndwr_patient_depression_screening`("build",1,10,1,"true");
#############################################################
replace into ndwr.ndwr_patient_contact_listing_build_queue(
        select distinct person_id from etl.flat_obs where encounter_type in (243)
        and encounter_datetime >= '2021-04-01'
);
CALL `ndwr`.`build_ndwr_patient_contact_listing`("build",1,1,1,"true");
####################################################################
replace into ndwr.ndwr_patient_eac_build_queue(
        select distinct person_id from etl.flat_obs where encounter_type in (2,106,129,110)
        and encounter_datetime >= '2021-04-01'
);
CALL `ndwr`.`build_ndwr_patient_eac`("build",1,1,1,"true");
####################################################################
replace into ndwr.ndwr_gbv_screening_build_queue(
        select distinct PatientPK from ndwr.ndwr_patient_contact_listing where DateCreated >= '2021-04-01'
);
CALL `ndwr`.`build_ndwr_gbv_screening`("build",1,1,1,"true");
####################################################################
replace into ndwr.ndwr_drug_alcohol_screening_build_queue(
SELECT 
    distinct person_id
FROM
    amrs.obs o
    join amrs.encounter e on (e.encounter_id = o.encounter_id)
    where o.concept_id in (5319) and e.encounter_type in (1)
    and o.date_created >= '2021-04-01 00:00:00'
);
CALL `ndwr`.`build_ndwr_drug_alcohol_screening`("build",1,1,1,"true");
####################################################################
replace into ndwr.ndwr_patient_allergies_chronic_illness_build_queue(
select distinct e.patient_id from amrs.obs o
join amrs.encounter e on (e.encounter_id = o.encounter_id)
where o.concept_id in (1120,2085,1122,1124,1125,1126,1129,1123,6011)
and e.encounter_type in (1) and e.encounter_datetime >= '2021-04-01 00:00:00'
);
CALL `ndwr`.`build_NDWR_patient_allergies_chronic_illness`("build",1,1,1,"true");
####################################################################
replace into ndwr_patient_ipt_extract_build_queue (
    select distinct person_id from etl.flat_hiv_summary_v15b where encounter_type in (1,2) 
    and e.encounter_datetime >= '2021-04-01 00:00:00'
);
CALL `ndwr`.`build_ndwr_patient_ipt_extract`("build", 1,1,1,"true");
####################################################################
replace into ndwr.ndwr_ovc_patient_visits_extract_build_queue(
 select                   
                              distinct pp.patient_id
                              from 
                              amrs.patient_program  pp
                              where pp.program_id in (2) AND pp.voided = 0
                              AND pp.location_id not in (195)
                              AND pp.date_created >= '2021-04-01 00:00:00'
                              group by pp.patient_id
);
