DELIMITER $$
CREATE  PROCEDURE `build_NDWR_ndwr_patient_labs_extract`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_labs_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_labs_extract_v1.1";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr.ndwr_patient_labs_extract (
  `PatientPK` INT NOT NULL,
  `SiteCode` INT NULL,
  `PatientID` VARCHAR(30) NULL,
  `FacilityID` INT NULL,
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
  `LabReason` VARCHAR(200) NULL,
  `DateSampleTaken` DATETIME NOT NULL,
  `SampleType` VARCHAR(200) NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY VisitID (VisitID),
   INDEX patient_order_date (PatientID , OrderedbyDate),
   INDEX patient_id (PatientID),
   INDEX patient_pk (PatientPK),
   INDEX dispense_date (OrderedbyDate),
   INDEX ordered_by_date_location (OrderedbyDate,FacilityID),
   INDEX patient_labs_site (SiteCode),
   INDEX patient_labs_site_visit (SiteCode,VisitID),
   INDEX patient_labs_site_patient_pk (SiteCode,PatientPK),
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
                             (select distinct person_id from etl.flat_lab_obs where max_date_created >= @last_update);
                             
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
                          
				CREATE temporary TABLE ndwr.ndwr_patient_labs_extract_interim(
                SELECT 
                            f.person_id as 'PatientPK',
                            t.SiteCode,
                            t.PatientID,
                            t.FacilityID,
                            t.Emr,
                            t.Project,
                            t.FacilityName,
                            NULL AS 'SatelliteName',
                            IF(f.encounter_id IS NOT NULL, f.encounter_id,f.obs_id) as 'VisitID',
                            IF(o.date_activated IS NOT NULL,o.date_activated,f.obs_datetime) AS 'OrderedbyDate',
                            f.obs_datetime as 'ReportedbyDate',
                            CASE
                            WHEN f.concept_id = 856 THEN 'VL'
                            WHEN f.concept_id = 730 THEN 'CD4 %'
                            WHEN f.concept_id = 5497 THEN 'CD4 Count'
                            ELSE NULL
                            END AS 'TestName',
                            NULL AS 'EnrollmentTest',
                            f.value_numeric as 'TestResult',
                            o.urgency AS 'LabReason',
                            DATE(f.obs_datetime) AS 'DateSampleTaken',
                            NULL AS 'Sample Type',
                            NULL AS 'DateCreated'
                        FROM
                            ndwr_patient_labs_extract_build_queue__0 q
                            join amrs.obs f on (f.person_id = q.person_id)
                            left join amrs.orders o on (f.order_id = o.order_id)
                            LEFT OUTER JOIN
                            amrs.obs `t7` ON (o.order_id = t7.order_id
                                AND (t7.voided IS NULL || t7.voided = 0)
                                AND t7.concept_id = 10189)
                            join ndwr.ndwr_all_patients_extract t on (t.PatientPK = f.person_id)
                        WHERE
                            f.concept_id in (856,730,5497)
                            and f.voided = 0
                           );
                          
                         
			
                          
						 SELECT CONCAT('Creating interim table .. ');


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_labs_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_labs_extract_interim i)');

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
