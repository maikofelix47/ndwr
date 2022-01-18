DELIMITER $$
CREATE  PROCEDURE `build_ndwr_patient_depression_screening`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_depression_screening";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_depression_screening_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr.ndwr_patient_depression_screening (
  `PatientPK` INT NOT NULL,
  `SiteCode` INT NOT NULL,
  `PatientID` VARCHAR(30) NULL,
  `FacilityID` INT NOT NULL,
  `FacilityName` VARCHAR(50) NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `VisitID` INT NULL,
  `VisitDate` DATETIME NULL,
  `PHQ9_1` tinyint NULL,
  `PHQ9_2`  tinyint NULL,
  `PHQ9_3` tinyint NULL,
  `PHQ9_4` tinyint NULL,
  `PHQ9_5` tinyint NULL,
  `PHQ9_6` tinyint NULL,
  `PHQ9_7` tinyint NULL,
  `PHQ9_8` tinyint NULL,
  `PHQ9_9` tinyint NULL,
  `PHQ9Score`  tinyint NULL,
  `PHQ9Rating`  tinyint NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY VisitID (VisitID),
   INDEX patient_ds_date (PatientID , VisitDate),
   INDEX patient_ds_id (PatientID),
   INDEX patient_ds_pk (PatientPK),
   INDEX patient_ds_site (SiteCode),
   INDEX patient_ds_site_visit_id (VisitID),
   INDEX ds_date_created (DateCreated)
);
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_depression_screening);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_patient_depression_screening_temp_",queue_number);
                            set @queue_table = concat("ndwr_patient_depression_screening_build_queue_",queue_number);                    												

										        SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_depression_screening_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_depression_screening_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  
                                          
					                  SET @person_ids_count = 0;
                            SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;

                            SELECT @person_ids_count AS 'num patients to build';
                   
                            SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ', @queue_table ,' t2 on (t2.person_id = t1.PatientPK);'); 
				                    SELECT CONCAT('Deleting patient records in interim ', @primary_table);
				                    PREPARE s1 from @dyn_sql; 
				                    EXECUTE s1; 
				                    DEALLOCATE PREPARE s1;  

				              end if;
                      if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_patient_depression_screening_temp_",queue_number);
                            set @queue_table = "ndwr_patient_depression_screening_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr_patient_depression_screening_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_patient_depression_screening_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b where date_created >= @last_update);

                      end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_depression_screening_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_depression_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_patient_depression_screening_interim;
                          
                          select concat('Creating ndwr_patient_depression_screening_interim table...');
                         
                          create temporary table ndwr_patient_depression_screening_interim (SELECT
                                    o.person_id AS 'PatientPK',
                                    mfl.mfl_code AS 'SiteCode',
                                    t.PatientID as 'PatientID',
                                    mfl.mfl_code AS 'FacilityID',
                                    mfl.Facility AS 'FacilityName',
									t.Emr as 'Emr',
							        t.Project as 'Project',
                                    o.encounter_id as 'VisitID',
                                    o.encounter_datetime as 'VisitDate',
                                        CASE
                                        WHEN o.obs REGEXP '!!7806=' THEN etl.GetValues(o.obs,7806)
                                        ELSE NULL
                                    END AS 'PHQ9_1',
                                    CASE
                                        WHEN o.obs REGEXP '!!7807=' THEN etl.GetValues(o.obs,7807)
                                        ELSE NULL
                                    END AS 'PHQ9_2',
                                    CASE
                                        WHEN o.obs REGEXP '!!7808=' THEN etl.GetValues(o.obs,7808)
                                        ELSE NULL
                                    END AS 'PHQ9_3',
                                    CASE
                                        WHEN o.obs REGEXP '!!7809=' THEN etl.GetValues(o.obs,7809)
                                        ELSE NULL
                                    END AS 'PHQ9_4',
                                    CASE
                                        WHEN o.obs REGEXP '!!7810=' THEN etl.GetValues(o.obs,7810)
                                        ELSE NULL
                                    END AS 'PHQ9_5',
                                    CASE
                                        WHEN o.obs REGEXP '!!7811=' THEN etl.GetValues(o.obs,7811)
                                        ELSE NULL
                                    END AS 'PHQ9_6',
                                    CASE
                                        WHEN o.obs REGEXP '!!7812=' THEN etl.GetValues(o.obs,7812)
                                        ELSE NULL
                                    END AS 'PHQ9_7',
                                    CASE
                                        WHEN o.obs REGEXP '!!7813=' THEN etl.GetValues(o.obs,7813)
                                        ELSE NULL
                                    END AS 'PHQ9_8',
                                    CASE
                                        WHEN o.obs REGEXP '!!7814=' THEN etl.GetValues(o.obs,7814)
                                        ELSE NULL
                                    END AS 'PHQ9_9',
                                    etl.GetValues(o.obs,7815) as 'PHQ9Score',
                                    CASE
                                        WHEN o.obs REGEXP '!!7815=' THEN etl.GetValues(o.obs,7815)
                                        ELSE NULL
                                    END AS 'PHQ9Rating',
                                NULL AS 'DateCreated'
                                FROM
                                    ndwr.ndwr_patient_depression_screening_build_queue__0 q
                                        JOIN
                                    etl.flat_obs o ON (q.person_id = o.person_id)
                                        JOIN
                                    ndwr.mfl_codes mfl ON (mfl.location_id = o.location_id)
                                    join ndwr.ndwr_all_patients_extract t on (t.PatientPK = q.person_id)
                                WHERE
                                    o.encounter_type IN (105,106,129,110,129,140,163,191)
                                        AND o.obs REGEXP '!!(7806|7807|7808|7809|7810|7811|7812|7813|7814)='
                                ORDER BY o.person_id,o.encounter_datetime ASC
                          );

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_depression_screening_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_depression_screening_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_depression_screening_build_queue__0 t2 using (person_id);'); 
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
