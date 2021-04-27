DELIMITER $$
CREATE  PROCEDURE `build_NDWR_ndwr_patient_labs_extract`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_labs_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_labs_extract_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr_patient_labs_extract (
  `PatientPK` INT NOT NULL,
  `PatientID` INT NOT NULL,
  `FacilityID` INT NULL,
  `SiteCode` INT NULL,
  `Emr` VARCHAR(50) NOT NULL,
  `Project` VARCHAR(50) NOT NULL,
  `FacilityName` VARCHAR(100) NOT NULL,
  `SatelliteName` VARCHAR(50) NULL,
  `VisitID` INT NULL,
  `OrderedbyDate` DATETIME NOT NULL,
  `ReportedbyDate` DATETIME NOT NULL,
  `TestName` VARCHAR(200) NULL,
  `EnrollmentTest` VARCHAR(50) NULL,
  `TestResult` INT NOT NULL,
  `Reason` VARCHAR(200) NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY VisitID (VisitID),
   INDEX patient_order_date (PatientID , OrderedbyDate),
   INDEX patient_id (PatientID),
   INDEX patient_pk (PatientPK),
   INDEX dispense_date (OrderedbyDate),
   INDEX ordered_by_date_location (OrderedbyDate,FacilityID),
   INDEX date_created (DateCreated)
);

                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_labs_extract);

                    if(@query_type="build") then

							        select 'BUILDING..........................................';
                                    set @write_table = concat("ndwr_patient_labs_extract_temp_",queue_number);
                                    set @queue_table = concat("ndwr_patient_labs_extract_build_queue_",queue_number);                    												

									SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							        PREPARE s1 from @dyn_sql; 
							        EXECUTE s1; 
							        DEALLOCATE PREPARE s1;  

							        SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_labs_extract_build_queue limit ', queue_size, ');'); 
							        PREPARE s1 from @dyn_sql; 
							        EXECUTE s1; 
							        DEALLOCATE PREPARE s1;  

							        SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_labs_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                    PREPARE s1 from @dyn_sql; 
							        EXECUTE s1; 
							        DEALLOCATE PREPARE s1;  
                                          
                                    SET @person_ids_count = 0;
                                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                                    PREPARE s1 from @dyn_sql; 
                                    EXECUTE s1; 
                                    DEALLOCATE PREPARE s1;

                                    SELECT @person_ids_count AS 'num patients to build';
                   
                   
                                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ', @queue_table ,' t2 on (t2.person_id = t1.PatientID);'); 
                                    SELECT CONCAT('Deleting patient records in interim ', @primary_table);
                                    PREPARE s1 from @dyn_sql; 
                                    EXECUTE s1; 
                                    DEALLOCATE PREPARE s1;  
                   

				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_patient_labs_extract_temp_",queue_number);
                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            set @queue_table = "ndwr_patient_labs_extract_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr.ndwr_patient_labs_extract_sync_queue (
                                 person_id INT(6) UNSIGNED,
                                 INDEX labs_sync_person_id (person_id)
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr.ndwr_patient_labs_extract_sync_queue
                             (select distinct person_id from etl.flat_lab_obs 
                             where 
                             obs REGEXP '!!5497=[0-9]'
                             AND DATE(max_date_created) >= @last_update)
                             ;

                             replace into ndwr.ndwr_patient_labs_extract_sync_queue
                             (select distinct person_id from etl.flat_lab_obs 
                             where 
                             obs REGEXP '!!730=[0-9]'
                             and DATE(max_date_created) >= @last_update)
                             ;

                             replace into ndwr.ndwr_patient_labs_extract_sync_queue
                             (select distinct person_id from etl.flat_lab_obs 
                             where 
                             obs REGEXP '!!856=[0-9]'
                             and DATE(max_date_created) >= @last_update)
                             ;
                             
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
					drop  table if exists ndwr_patient_labs_extract_build_queue__0;

					SET @dyn_sql=CONCAT('create table if not exists ndwr_patient_labs_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                                      
						  
					drop temporary table if exists ndwr_patient_labs_extract_interim;
                          
				CREATE temporary TABLE ndwr.ndwr_patient_labs_extract_interim
                   (SELECT 
                       t.person_id AS PatientPK,
                       t.person_id AS PatientID,
                       t.VisitID,
                       t.test_datetime as OrderedbyDate,
                       t.test_datetime as ReportedbyDate,
                       t.TestName,
					   null AS EnrollmentTest,
					   t.TestResult,
                       t.TestName as Reason
                      
                           
                   FROM
                       (SELECT 
                           t1.person_id,
                               t1.test_datetime,
                               'CD4 Count' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t1.obs,
                                                                           LOCATE('!!5497=', t1.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!5497=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED) AS TestResult,
                               encounter_id as VisitID
   
                       FROM
                       etl.flat_lab_obs t1 
                       join ndwr.ndwr_patient_labs_extract_build_queue__0 b1 on (b1.person_id = t1.person_id)
  					 WHERE 			 
  					 t1.obs REGEXP '!!5497=[0-9]' 
                           
                           
                           UNION  
                           
                           SELECT 
                           t2.person_id,
                               t2.test_datetime,
                               'CD4 %' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t2.obs,
                                                                           LOCATE('!!730=', t2.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!730=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED)   AS TestResult,
                               t2.encounter_id as VisitID
   
                       FROM
                           etl.flat_lab_obs t2   
                           join ndwr.ndwr_patient_labs_extract_build_queue__0 b2 on (b2.person_id = t2.person_id)
                           WHERE  t2.obs REGEXP '!!730=[0-9]'
                           
                           Union 
                           SELECT 
                           t3.person_id,
                           t3.test_datetime,
                               'VL' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t3.obs,
                                                                           LOCATE('!!856=', t3.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!856=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED) AS TestResult,
                               t3.encounter_id as VisitID
   
                       FROM
                           etl.flat_lab_obs  t3  
						   join ndwr.ndwr_patient_labs_extract_build_queue__0 b3 on (b3.person_id = t3.person_id)
                           WHERE  
                           t3.obs REGEXP '!!856=[0-9]'
                           
                           ) t 
                           );
                          
                         
			
                          
						 SELECT CONCAT('Creating interim table .. ');


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_labs_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select 
                           i.PatientPK,
                           i.PatientID,
						   t.FacilityID AS FacilityID,
                           t.SiteCode AS SiteCode,
					       "AMRS" AS Emr,
					       "Ampath Plus" AS Project,
                           t.FacilityName AS FacilityName,
                           null as SatelliteName,
                           i.VisitID,
                           i.OrderedbyDate,
                           i.ReportedbyDate,
                           i.TestName,
					       i.EnrollmentTest,
					       i.TestResult,
                           i.Reason,
                           null as DateCreated
                          
                          from ndwr_patient_labs_extract_interim i
                          join ndwr.ndwr_all_patients_extract t on (t.PatientID = i.PatientID)
                          
                          )');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_labs_extract_build_queue__0 t2 using (person_id);'); 
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
