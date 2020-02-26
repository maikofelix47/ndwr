CREATE  PROCEDURE `buildNDWRFacilityData`()
BEGIN
   DECLARE n INT DEFAULT 0;
          DECLARE i INT DEFAULT 0;
  		 DECLARE selectedMFLCode INT Default 0;
  		 DECLARE selectedPatient INT Default 0;
  		 DECLARE selectedFacility varchar(200) Default 0;        
  		 DECLARE selectedPeriod date;
		SELECT 
    reporting_period
FROM
    ndwr.mfl_period
LIMIT 1 INTO selectedPeriod;
		SELECT 
    mfl_code
FROM
    ndwr.mfl_period
LIMIT 1 INTO selectedMFLCode;
  		SELECT 
    facility
FROM
    ndwr.mfl_codes
WHERE
    mfl_code = selectedMFLCode
LIMIT 1 INTO selectedFacility;
DELETE FROM ndwr.ndwr_baseline_queue 
WHERE
    person_id IN (SELECT 
        patientid
    FROM
        ndwr.ndwr_base_line);
 		SELECT 
    COUNT(*)
FROM
    ndwr.ndwr_baseline_queue INTO n;
 		 if n <= 0 then
 			 truncate table  ndwr.ndwr_baseline_queue;						
 				  replace into ndwr.ndwr_baseline_queue (
 				  select person_id from etl.hiv_monthly_report_dataset_v1_2 where  enddate=selectedPeriod and location_id in(select location_id from ndwr.mfl_codes where mfl_code=selectedMFLCode)
                             
 			 );
 			DELETE FROM ndwr.ndwr_patient_labs_extract;
			DELETE FROM ndwr.ndwr_patient_art_extract;
			DELETE FROM ndwr.ndwr_patient_status;
			DELETE FROM ndwr_all_patient_visits_extract;
			DELETE FROM ndwr_all_patients_extract;
			DELETE FROM ndwr_all_patients;
			DELETE FROM ndwr.ndwr_patient_pharmacy;
			DELETE FROM ndwr.ndwr_patient_status_extract;
			DELETE FROM ndwr.ndwr_vitals;
			DELETE FROM ndwr.patient_base_line;
			DELETE FROM ndwr.ndwr_base_line;
			DELETE FROM ndwr.ndwr_patient_baselines_extract;
			DELETE FROM ndwr.ndwr_patient_adverse_events;
 			DELETE FROM base_temp_1;
 			DELETE FROM progress;
 			SELECT 
    COUNT(*)
FROM
    ndwr.ndwr_baseline_queue INTO n;
 			 
 		 end if;
 	                     	
          SET i=0;
  		
 
          WHILE i<n DO  		
 				SELECT distinct person_id  FROM ndwr_baseline_queue limit i,1 into selectedPatient; 
 				
 				call createPatientNDWRDataSets(selectedMFLCode,selectedFacility,selectedPatient);	
  				insert into progress(status,queue) select  concat(((i/n) * 100),' % of ', n) as '% complete', 1 as queue;
 				           	   
            SET i = i + 1;
   end while;
DELETE FROM ndwr.ndwr_baseline_queue;             
  END