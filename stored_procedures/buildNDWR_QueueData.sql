DELIMITER $$
CREATE  PROCEDURE `buildNDWR_QueueData`(IN queuNumber INT)
BEGIN
   
          DECLARE i INT DEFAULT 0;
  		 DECLARE selectedFacility varchar(200) Default 0;
  		 DECLARE selectedMFLCode INT Default 0;
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
SELECT @beforeCal:=NOW();
  		SELECT 
    facility
FROM
    ndwr.mfl_codes
WHERE
    mfl_code = selectedMFLCode
LIMIT 1 INTO selectedFacility;         
		 set @dyn_sql=concat("delete from ndwr_baseline_queue_",queuNumber," where person_id in(select patientid from ndwr.ndwr_base_line)");
		 PREPARE s1 from @dyn_sql; 
		 EXECUTE s1; 
		 DEALLOCATE PREPARE s1;
         set @n= 0;
         
         set @selectedPatient= null;
		 set @dyn_sql= concat('SELECT @n := count(*) from ndwr_baseline_queue_',queuNumber);


		 PREPARE s1 from @dyn_sql; 
		 EXECUTE s1; 
		 DEALLOCATE PREPARE s1;  
         
         SELECT @n uuuuuuuuuuuuuuuu;
		
          WHILE i<@n DO  		
             set @dyn_sql=concat(" SELECT distinct @selectedPatient := person_id  FROM ndwr_baseline_queue_",queuNumber," limit @n, 1");
			 PREPARE s1 from @dyn_sql; 
			 EXECUTE s1; 
			 DEALLOCATE PREPARE s1; 
               SELECT selectedMFLCode ppppppppppppp,selectedFacility oooooooooooo,@selectedPatient rrrrrrrrrrrrrrrrrrrrrrrrr;
 				call createPatientNDWRDataSets(selectedMFLCode,selectedFacility,@selectedPatient);	
  				insert into progress(status,queue) select  concat(((i/@n) * 100),' % of ', @n) as '% complete', queuNumber as queue;
 				SET i = i + 1;
		end while; 
    
		 set @dyn_sql= concat('delete from ndwr_baseline_queue_',queuNumber);	
		 #PREPARE s1 from @dyn_sql; 
		 #EXECUTE s1; 
		 #DEALLOCATE PREPARE s1;
  END$$
DELIMITER ;
