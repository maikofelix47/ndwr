DELIMITER $$
CREATE  PROCEDURE `build_NDWR_all_patient_status_extract`(IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_all_patient_status_extract";
          set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_all_patient_status_extract_v1.0";
          set @query_type="build";


CREATE TABLE IF NOT EXISTS `ndwr`.`ndwr_patient_status_extract` (
    `PatientPK` INT NOT NULL,
    `PatientID` INT NOT NULL,
    `FacilityId` INT NOT NULL,
    `SiteCode` VARCHAR(50) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(50) NOT NULL,
    `ExitDescription` VARCHAR(50) NULL,
    `ExitDate` DATETIME NULL,
    `ExitReason` VARCHAR(200) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     INDEX status_patient_id (PatientID),
     INDEX status_patient_pk (PatientPK),
     INDEX status_facility_id (FacilityID),
     INDEX status_site_code (SiteCode),
     INDEX status_date_created (DateCreated),
     INDEX status_patient_facility (PatientID,FacilityID)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_all_patient_status_extract_temp_",queue_number);
                            set @queue_table = concat("ndwr_all_patient_status_extract_build_queue_",queue_number);                    												

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_all_patient_status_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_all_patient_status_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  
                                          
					SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

SELECT @person_ids_count AS 'num patients to build';

				  end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_all_patient_status_extract_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_all_patient_status_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						SELECT CONCAT('Deleting data from ', @primary_table);
                                      
						 SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ndwr_all_patient_status_extract_build_queue__0 t2 on (t1.PatientPK = t2.person_id);'); 
						 PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
					                DEALLOCATE PREPARE s1; 
                                      
						  
                drop temporary table if exists ndwr_all_patient_status_extract_interim;

SELECT CONCAT('Creating and populating interim status table ..');
                          
                create temporary table ndwr_all_patient_status_extract_interim (
                     select
                   t1.PatientPK,
                   t1.PatientID,
                   t1.FacilityId,
                   t1.SiteCode,
				           t1.Emr,
                   t1.Project,
                   t1.FacilityName,
  				         if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitDescription,
				           if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),t1.lastVisit,null) as ExitDate,
                   if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitReason
                 
                    from ndwr.ndwr_all_patients t1
                    inner join ndwr.ndwr_all_patient_status_extract_build_queue__0 t2 on (t1.PatientID = t2.person_id)
                     where t1.StatusAtCCC in('dead','ltfu','transfer_out')
					);
                          
						SELECT CONCAT('Created interim status table ..');


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_all_patient_status_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_all_patient_status_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_all_patient_status_extract_build_queue__0 t2 using (person_id);'); 
					                PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
					                DEALLOCATE PREPARE s1;  
                        

						 SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
                         PREPARE s1 from @dyn_sql; 
                         EXECUTE s1; 
                         DEALLOCATE PREPARE s1;
                         
                         set @cycle_length = timestampdiff(second,@loop_start_time,now());
                         set @total_time = @total_time + @cycle_length;
                         set @cycle_number = @cycle_number + 1;
                         set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                         
SELECT 
    @person_ids_count AS 'persons remaining',
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';


                    end while;

                         SET @dyn_sql=CONCAT('drop table ',@queue_table,';');
                         PREPARE s1 from @dyn_sql;
                         EXECUTE s1;
                         DEALLOCATE PREPARE s1;  

                         SET @total_rows_to_write=0;
                         SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
                         PREPARE s1 from @dyn_sql; 
                         EXECUTE s1; 
                         DEALLOCATE PREPARE s1;

                         set @start_write = now();
                         
SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

                        SET @dyn_sql=CONCAT('replace into ', @primary_table,'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1;
                        DEALLOCATE PREPARE s1;

SELECT 
    CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');
            
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        
                        set @ave_cycle_length = ceil(@total_time/@cycle_number);
SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            'second(s)');
                        set @end = now();
                        
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');


END$$
DELIMITER ;
