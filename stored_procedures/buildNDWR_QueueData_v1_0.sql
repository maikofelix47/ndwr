DELIMITER $$
CREATE  PROCEDURE `buildNDWR_QueueData_v1_0`(IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
 SET primary_table := "flat_hiv_summary_v15b";
 SET @i := 0;
 SET @n := 0;
 SET @selectedMFLCode := 0;
 SET @selectedPatient := 0 ;
 SET @selectedFacility := '';        
 SET @selectedPeriod := null ;        
 Select reporting_period from ndwr.mfl_period limit 1 into @selectedPeriod;
 Select mfl_code from ndwr.mfl_period limit 1 into @selectedMFLCode;
 Select facility from ndwr.mfl_codes where mfl_code = @selectedMFLCode limit 1 into @selectedFacility;
 set @queue_table = concat("ndwr_baseline_test_build_queue_",queue_number);


  SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_baseline_queue_test limit ', queue_size, ');'); 
  PREPARE s1 from @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  
                            
                            
  SET @dyn_sql=CONCAT('delete t1 from ndwr_baseline_queue_test t1 join ',@queue_table, ' t2 using (person_id);'); 
  PREPARE s1 from @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  
  
  
  SET @person_ids_count = 0;
  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
  PREPARE s1 from @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;
  
  SELECT @person_ids_count AS 'num patients to build';
        

SET i=0;
  		
 
          WHILE i<n DO
          set @loop_start_time = now();
          SET @person_ids_count = 0;
          SET @dyn_sql=CONCAT('select distinct person_id  FROM ',@queue_table,'limit 1','into',@selectedPatient,';'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
		  DEALLOCATE PREPARE s1;
 				
 				call createPatientNDWRDataSets(@selectedMFLCode,@selectedFacility,@selectedPatient);
  				
 				           	   
            SET i = i + 1;
            set @cycle_length = timestampdiff(second,@loop_start_time,now());
			set @total_time = @total_time + @cycle_length;
			set @cycle_number = @cycle_number + 1;
			set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
			SELECT @person_ids_count AS 'persons remaining', @cycle_length AS 'Cycle time (s)', CEIL(@person_ids_count / cycle_size) AS remaining_cycles, @remaining_time AS 'Est time remaining (min)';
            
		   SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' where person_id =',@selectedPatient,';'); 
           PREPARE s1 from @dyn_sql; 
		   EXECUTE s1; 
		   DEALLOCATE PREPARE s1;  
  
  
  end while; 
  END$$
DELIMITER ;
