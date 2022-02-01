DELIMITER $$
CREATE  PROCEDURE `build_ndwr_mnch_mother_baby_pair_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_mnch_mother_baby_pair_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_mnch_mother_baby_pair_extract_v1.0";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_mnch_mother_baby_pair_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_mnch_mother_baby_pair_extract (
    `BabyPatientPK` INT NOT NULL,
    `MotherPatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientHEI_ID` VARCHAR(30) NULL,
    `MotherMNCH_ID` VARCHAR(30) NOT NULL,
    `PatientIDCCC` VARCHAR(30) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX mother_baby_pair_baby_patient_pk (BabyPatientPK),
    INDEX mother_baby_pair_baby_mother_pk (MotherPatientPK),
    INDEX mother_baby_pair_site_code (SiteCode),
    INDEX mother_baby_pair_date_created (DateCreated),
    INDEX mnch_patient_baby_patient_site_code (BabyPatientPK , SiteCode),
    INDEX mnch_patient_baby_mother_site_code (MotherPatientPK , SiteCode)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_mnch_patient_extract_build_queue_temp_",queue_number);
                            set @queue_table = concat("ndwr_mnch_patient_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_mnch_mother_baby_pair_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_mnch_mother_baby_pair_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                           

                  end if;
                  
                  SET @person_ids_count = 0;
				  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
				  PREPARE s1 from @dyn_sql; 
				  EXECUTE s1; 
				  DEALLOCATE PREPARE s1;

SELECT @person_ids_count AS 'num patients to build';
                  
SELECT CONCAT('Deleting data from ', @primary_table);
                    
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 on (t1.BabyPatientPK = t2.person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                    set @total_time=0;
                    set @cycle_number = 0;
                    set @last_encounter_date=null;
                    set @status=null;                            
					set @last_encounter_date=null;                            
					set @rtc_date=null; 

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop  table if exists ndwr_mnch_mother_baby_pair_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_mnch_mother_baby_pair_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ', cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;

                        
                        

						SELECT CONCAT('Creating ndwr_mnch_mother_baby_pair_extract_interim table ...');
                                      
						  
                          drop temporary table if exists ndwr_mnch_mother_baby_pair_extract_interim;
                          create temporary table ndwr_mnch_mother_baby_pair_extract_interim (
                          SELECT
						   f.person_id as 'PatientPK',
                           f.mother_person_id as 'MotherPatientPK',
                           mfl.mfl_code as 'SiteCode',
						   hei.identifier as 'PatientHEI_ID',
                           mother_amrs.identifier as 'MotherMNCH_ID',
                           REPLACE(mother_ccc.identifier, "-", "") as 'PatientIDCCC',
						   'AMRS' AS Emr,
						   'Ampath Plus' AS 'Project',
						   mfl.Facility AS FacilityName,
                           NULL AS DateCreated
                            FROM 
                            ndwr.ndwr_mnch_mother_baby_pair_build_queue__0 q
                            JOIN 
                            etl.flat_hei_summary f ON (f.person_id = q.person_id)
                            left join amrs.patient_identifier mother_ccc on ( mother_ccc.patient_id = f.mother_person_id AND mother_ccc.identifier_type = 28 AND mother_ccc.voided = 0)
                            left join amrs.patient_identifier hei on (hei.patient_id = q.person_id AND hei.identifier_type = 38 AND hei.voided = 0)
							left join amrs.patient_identifier mother_amrs on (mother_amrs.patient_id = f.mother_person_id AND  mother_amrs.identifier_type = 8 AND  mother_amrs.voided = 0)
                            join ndwr.mfl_codes mfl on (f.location_id = mfl.location_id)
                            WHERE
                            f.is_clinical_encounter = 1
                            AND f.next_clinical_datetime_hiv IS NULL
                          );
                          


                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_mnch_mother_baby_pair_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_mnch_mother_baby_pair_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_mnch_mother_baby_pair_build_queue__0 t2 using (person_id);'); 
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
                        
insert into ndwr.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                        
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');


END$$
DELIMITER ;
