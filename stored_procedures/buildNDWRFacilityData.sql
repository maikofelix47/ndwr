USE `ndwr`;
DROP procedure IF EXISTS `buildNDWRFacilityData`;

DELIMITER $$
USE `ndwr`$$
CREATE DEFINER=`fmaiko`@`%` PROCEDURE `buildNDWRFacilityData`()
BEGIN
   DECLARE n INT DEFAULT 0;
          DECLARE i INT DEFAULT 0;
  		 DECLARE selectedMFLCode INT Default 0;
  		 DECLARE selectedPatient INT Default 0;
  		 DECLARE selectedFacility varchar(200) Default 0;        
  		 DECLARE selectedPeriod date;        
		  Select reporting_period from ndwr.mfl_period limit 1 into selectedPeriod;
		  Select mfl_code from ndwr.mfl_period limit 1 into selectedMFLCode;
  		  Select facility from ndwr.mfl_codes where mfl_code = selectedMFLCode limit 1 into selectedFacility;
          delete from ndwr.ndwr_baseline_queue where person_id in(select patientid from ndwr.ndwr_base_line);
 		 SELECT count(*) from ndwr.ndwr_baseline_queue  INTO n;
 		 if n <= 0 then
 			 truncate table  ndwr.ndwr_baseline_queue;						
 				  replace into ndwr.ndwr_baseline_queue (
 				  select person_id from etl.hiv_monthly_report_dataset_v1_2 where  enddate=selectedPeriod and location_id in(select location_id from ndwr.mfl_codes where mfl_code=selectedMFLCode)
                             
 			 );
 			# clean tables before processing 
              delete from ndwr.ndwr_patient_labs_extract;
              delete from ndwr.ndwr_patient_art_extract;
              delete from ndwr.ndwr_patient_status;
              delete from ndwr_all_patient_visits_extract;
              delete from ndwr_all_patients_extract;
              delete from ndwr_all_patients;
              delete from ndwr.ndwr_patient_pharmacy;
              delete from ndwr.ndwr_patient_status_extract;
              delete from ndwr.ndwr_vitals; 
              delete FROM ndwr.patient_base_line;
              delete FROM ndwr.ndwr_base_line;
			  delete FROM ndwr.ndwr_patient_baselines_extract;
 			 delete from base_temp_1;
 			 delete from progress;
 			 SELECT count(*) from ndwr.ndwr_baseline_queue  INTO n;
 			 
 		 end if;
 	                     	
          SET i=0;
  		
 
          WHILE i<n DO  		
 				SELECT distinct person_id  FROM ndwr_baseline_queue limit i,1 into selectedPatient; 
 				
 				call createPatientNDWRDataSets(selectedMFLCode,selectedFacility,selectedPatient);	
  				insert into progress(status,queue) select  concat(((i/n) * 100),' % of ', n) as '% complete', 1 as queue;
 				           	   
            SET i = i + 1;
   end while; 
   delete from ndwr.ndwr_baseline_queue;             
  END$$

DELIMITER ;

