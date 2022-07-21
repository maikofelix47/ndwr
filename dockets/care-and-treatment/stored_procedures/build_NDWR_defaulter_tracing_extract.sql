DELIMITER $$
CREATE  PROCEDURE `build_NDWR_defaulter_tracing_extract`(IN query_type varchar(50) , IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_defaulter_tracing_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_defaulter_tracing_extract_v1.0";
                    set @query_type = query_type;

CREATE TABLE IF NOT EXISTS `ndwr`.`ndwr_defaulter_tracing_extract` (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NULL,
    `PatientID` VARCHAR(30) NULL,
    `Emr` VARCHAR(10) NULL,
    `Project` VARCHAR(20) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `VisitID` INT NOT NULL,
    `VisitDate` DATETIME NOT NULL,
    `EncounterId` INT NOT NULL,
    `TracingType` VARCHAR(30) NULL,
    `TracingOutcome` VARCHAR(30) NULL,
    `AttemptNumber` INT NULL,
    `IsFinalTrace` TINYINT NULL,
    `TrueStatus` VARCHAR(30) NULL,
    `ReasonForMissedAppointment` VARCHAR(30) NULL,
    `CauseOfDeath` VARCHAR(30) NULL,
    `Comments` VARCHAR(250) NULL,
    `BookingDate` DATETIME NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX defaulter_patient_id (PatientID),
    INDEX defaulter_patient_pk (PatientPK),
    INDEX defaulter_site_code (SiteCode),
    INDEX defaulter_visit_date (VisitDate),
    INDEX defaulter_visit_id (VisitID),
    INDEX defaulter_encounter_id (EncounterId),
    INDEX defaulter_date_created (DateCreated),
    INDEX defaulter_patient_visit_date (PatientID , VisitDate),
    INDEX defaulter_patient_visit_id (PatientID , VisitID),
    INDEX defaulter_patient_site_code (PatientID , SiteCode)
);

                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_defaulter_tracing_extract);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_defaulter_tracing_extract_temp_",queue_number);
                            set @queue_table = concat("ndwr_defaulter_tracing_extract_build_queue_",queue_number);                    												

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_defaulter_tracing_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_defaulter_tracing_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
          if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_defaulter_tracing_extract_temp_",queue_number);
							SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
                                          
                            set @queue_table = "ndwr_defaulter_tracing_extract_sync_queue";
CREATE TABLE IF NOT EXISTS ndwr.ndwr_defaulter_tracing_extract_sync_queue (
    person_id INT(6) UNSIGNED,
    INDEX defaulter_sync_person_id (person_id)
);                            
                            
                            set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    ndwr.flat_log
WHERE
    table_name = @table_version;

                            replace into ndwr.ndwr_defaulter_tracing_extract_sync_queue
                             (select distinct person_id from etl.flat_obs where encounter_type in (21) and date_created >= @last_update);
                             
                             SET @person_ids_count = 0;
							 SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
							 PREPARE s1 from @dyn_sql; 
							 EXECUTE s1; 
							 DEALLOCATE PREPARE s1;

							SELECT @person_ids_count AS 'num patients to sync';

            end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_defaulter_tracing_extract_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_defaulter_tracing_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						SELECT CONCAT('Deleting data from ', @primary_table);
                                      
						 SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ndwr_defaulter_tracing_extract_build_queue__0 t2 on (t1.PatientPK = t2.person_id);'); 
						 PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
					                DEALLOCATE PREPARE s1;
                                    
                                    
                                    
SELECT CONCAT('Creating missed appointments reasons .... ');
                                    
                                    drop temporary table if exists ndwr_missed_appointment_reasons;
                                    
                                    create temporary table ndwr_missed_appointment_reasons(
									  SELECT 
										o.person_id,
										o.encounter_id,
										o.concept_id,
										o.value_coded,
										GROUP_CONCAT(DISTINCT cn.name SEPARATOR '  |  ') AS `reason_for_missed_appointment`
									FROM
                                        ndwr_defaulter_tracing_extract_build_queue__0 q
                                        join etl.flat_obs fo on (fo.person_id = q.person_id)
										join amrs.obs o on (o.person_id = fo.person_id AND o.encounter_id = fo.encounter_id)
										join amrs.concept_name cn on (cn.concept_id = o.value_coded AND cn.voided = 0 AND cn.locale_preferred = 1)
									WHERE
										fo.encounter_type in (21)
										and o.concept_id in (10350,10354,10359)
										group by o.person_id,o.encounter_id);
                                    
                                    
			   
                 drop temporary table if exists ndwr_defaulter_tracing_extract_interim;

SELECT CONCAT('Creating and populating interim table ..');
                 create temporary table ndwr_defaulter_tracing_extract_interim (
                       SELECT distinct
						   e.person_id AS 'PatientPK',
                           mfl.mfl_code as 'SiteCode',
                           a.PatientID AS 'PatientID',
                           'AMRS' AS 'Emr',
						   'Ampath Plus' AS 'Project',
                           mfl.Facility AS 'FacilityName',
                           e.encounter_id AS 'VisitID',
                           e.encounter_datetime AS 'VisitDate',
						   e.encounter_id as EncounterId,
                           CASE
								WHEN e.obs REGEXP '!!1558=1555' THEN 'Phone follow up'
                                WHEN e.obs REGEXP '!!1558=7066' THEN 'Home visit'
								ELSE NULL
						   END AS 'TracingType',
                            CASE
								WHEN e.obs REGEXP '!!9600=1065' THEN 'Contact'
                                WHEN e.obs REGEXP '!!9600=1066' THEN 'No Contact'
								WHEN e.obs REGEXP '!!1559=1065' THEN 'Contact'
                                WHEN e.obs REGEXP '!!1559=1066' THEN 'No Contact'
								ELSE NULL
						   END AS 'TracingOutcome',
                           1 as AttemptNumber,
                           null as IsFinalTrace,
                           null as TrueStatus,
                           mr.reason_for_missed_appointment as ReasonForMissedAppointment,
                           CASE
								WHEN e.obs REGEXP '!!1573=58' THEN 'TUBERCULOSIS'
                                WHEN e.obs REGEXP '!!1573=6483' THEN 'CANCER'
                                WHEN e.obs REGEXP '!!1573=123' THEN 'Infectious and parasitic disease (malaria, typhoid, cholera)'
                                WHEN e.obs REGEXP '!!1573=10365' THEN 'NON INFECTIOUS HIV RELATED DISEASE'
                                WHEN e.obs REGEXP '!!1573=903' THEN 'Other natural causes (heart disease, diabetes, hypertension)'
                                WHEN e.obs REGEXP '!!1573=1572' THEN 'Non-natural causes (accident, murder, suicide, war, trauma)'
								WHEN e.obs REGEXP '!!1573=6483' THEN 'UNKNOWN'
								ELSE NULL
						   END AS 'CauseOfDeath',
                           CASE
								WHEN e.obs REGEXP '!!9467=' THEN etl.GetValues(e.obs,9467)
								ELSE NULL
						   END AS 'Comments',
                            CASE
								WHEN e.obs REGEXP '!!5096=' THEN etl.GetValues(e.obs,5096)
								ELSE NULL
						   END AS 'BookingDate',
                           null as 'DateCreated'
                           FROM 
                           ndwr_defaulter_tracing_extract_build_queue__0 t3 
                           join etl.flat_obs e on (t3.person_id = e.person_id)
                           join ndwr.mfl_codes mfl on (mfl.location_id = e.location_id)
                           join ndwr.ndwr_all_patients_extract a on (a.PatientPK = e.person_id)
                           left join ndwr_missed_appointment_reasons mr on (mr.encounter_id = e.encounter_id)
                           WHERE e.encounter_type IN (21)
                       );
                          
                
                          


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_defaulter_tracing_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_defaulter_tracing_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_defaulter_tracing_extract_build_queue__0 t2 using (person_id);'); 
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
