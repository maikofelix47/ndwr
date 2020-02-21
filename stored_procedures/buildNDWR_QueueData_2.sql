CREATE  PROCEDURE `buildNDWR_QueueData_2`()
BEGIN
 DECLARE i INT Default 0;
 DECLARE n INT Default 0;
 DECLARE selectedMFLCode INT Default 0;
  		 DECLARE selectedPatient INT Default 0;
  		 DECLARE selectedFacility varchar(200) Default 0;        
  		 DECLARE selectedPeriod date;        
		  Select reporting_period from ndwr.mfl_period limit 1 into selectedPeriod;
		  Select mfl_code from ndwr.mfl_period limit 1 into selectedMFLCode;
  		  Select facility from ndwr.mfl_codes where mfl_code = selectedMFLCode limit 1 into selectedFacility;
          delete from ndwr.ndwr_baseline_queue_2 where person_id in(select patientid from ndwr.ndwr_base_line);
 		 SELECT count(*) from ndwr.ndwr_baseline_queue_2  INTO n;
        

 
SET i=0;
  		
 
          WHILE i<n DO  		
 				SELECT distinct person_id  FROM ndwr_baseline_queue_2 limit i,1 into selectedPatient; 
 				
 				call createPatientNDWRDataSets(selectedMFLCode,selectedFacility,selectedPatient);	
  				insert into progress(status,queue) select  concat(((i/n) * 100),' % of ', n) as '% complete', 2 as queue;
 				           	   
            SET i = i + 1;
   end while; 
   delete from ndwr.ndwr_baseline_queue_2;    
		 #PREPARE s1 from @dyn_sql; 
		 #EXECUTE s1; 
		 #DEALLOCATE PREPARE s1;
  END