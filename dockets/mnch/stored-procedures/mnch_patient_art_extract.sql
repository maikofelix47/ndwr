DELIMITER $$
CREATE  PROCEDURE `build_ndwr_mnch_patient_art_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_mnch_patient_art_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_mnch_patient_art_extract_v1.0";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_mnch_patient_art_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_mnch_patient_art_extract (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientMNCH_ID` VARCHAR(30) NOT NULL,
    `PatientHEI_ID` VARCHAR(30) NULL,
    `PatientID` VARCHAR(30) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `RegistrationAtCCC` DATETIME NOT NULL,
    `StartARTDate` DATETIME NOT NULL,
    `StartRegimen` VARCHAR(100) NULL,
    `StartRegimenLine` VARCHAR(100) NULL,
    `StatusAtCCC` VARCHAR(100) NULL,
    `DateStartedCurrentRegimen` DATETIME NULL,
    `LastRegimen` VARCHAR(100) NULL,
    `LastRegimenLine` VARCHAR(100) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX mnch_patient_art_patient_pk (PatientPK),
    INDEX mnch_patient_art_site_code (SiteCode),
    INDEX mnch_patient_art_id (PatientID),
    INDEX mnch_patient_art_date_created (DateCreated),
    INDEX mnch_patient_patient_art_site_code (PatientPK , SiteCode),
    INDEX mnch_patient_patient_art_patient_id_site_code (PatientID , SiteCode)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_mnch_patient_extract_build_queue_temp_",queue_number);
                            set @queue_table = concat("ndwr_mnch_patient_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_mnch_patient_art_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_mnch_patient_art_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
                    
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 on (t1.PatientPK = t2.person_id);'); 
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
							drop  table if exists ndwr_mnch_patient_art_extract_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_mnch_patient_art_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ', cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;

                        
                        

						SELECT CONCAT('Creating ndwr_mnch_patient_art_extract_interim table ...');
                                      
						  
                          drop temporary table if exists ndwr_mnch_patient_art_extract_interim;
                          create temporary table ndwr_mnch_patient_art_extract_interim (
                          SELECT
                           p.PatientPK,
                           p.SiteCode,
                           p.PatientMNCH_ID,
                           p.PatientHEI_ID,
                           REPLACE(c.identifier, "-", "") as PatientID,
						   p.Emr,
						   p.Project,
						   p.FacilityName,
						   f.enrollment_date as 'RegistrationAtCCC',
                           f.arv_first_regimen_start_date as StartARTDate,
                           f.arv_first_regimen as StartRegimen,
						   f.cur_arv_line AS StartRegimenLine,
                           case
                                when date_format(@endDate, "%Y-%m-01") > f.death_date then @status := "dead"
                                when date_format(@endDate, "%Y-%m-01") > date_format(transfer_out_date, "%Y-%m-01") then @status := "transfer_out"
                                when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 28 day)),@endDate) <= 28 then @status := "active"
                                when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 28 day)),@endDate) between 29 and 90 then @status := "defaulter"
                                when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 28 day)),@endDate) > 90 then @status := "ltfu"
                                else @status := "unknown"
                          end as  'StatusAtCCC',
						   f.arv_start_date as 'DateStartedCurrentRegimen',
						   f.cur_arv_meds as 'LastRegimen',
						   f.cur_arv_line as 'LastRegimenLine',
                           NULL AS DateCreated
                            FROM 
                            ndwr.ndwr_mnch_patient_art_extract_build_queue__0 q
                            JOIN 
                            etl.flat_hiv_summary_v15b f ON (f.person_id = q.person_id)
                            left JOIN
                            ndwr.ndwr_mnch_patients_extract p on (f.person_id = p.PatientPK)
                            left join amrs.patient_identifier c on (c.patient_id = q.person_id AND c.identifier_type = 28 AND c.voided = 0)
                            WHERE
                            f.is_clinical_encounter = 1
                            AND f.next_clinical_datetime_hiv IS NULL
                          );
                          


                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_mnch_patient_art_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_mnch_patient_art_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_mnch_patient_art_extract_build_queue__0 t2 using (person_id);'); 
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
