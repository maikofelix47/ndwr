DELIMITER $$
CREATE  PROCEDURE `build_ndwr_all_anc_patients_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN end_date varchar(50) ,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_all_anc_patients_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_all_anc_patients_v1.1";
                    set @query_type=query_type;
                    set @end_date = end_date;
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_all_anc_patients_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_all_anc_patients_extract (
    `PKV` INT NULL,
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientID` INT NOT NULL,
    `FacilityID` INT NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `EnrolmentDate` DATETIME NOT NULL,
    `ServiceType` VARCHAR(10) NULL,
    `GestationWeeks` INT NULL,
    `Parity` VARCHAR(10) NULL,
    `Gravidae` INT NULL,
    `LMP` DATE NULL,
    `EDD` DATE,
    `GestationInWeeks` INT NULL,
    `Height` INT NULL,
    `Weight` INT NULL,
    `Temp` INT NULL,
    `PulseRate` INT NULL,
    `RespiratoryRate` INT NULL,
    `OxygenSaturation` INT NULL,
    `MUAC` INT NULL,
    `BP`VARCHAR(20) NULL,
    `BreastExam` VARCHAR(10) NULL,
    `CounselledOn` VARCHAR(200) NULL,
    `Haemoglobin` VARCHAR(200) NULL,
    `RPR_VDRL` VARCHAR(200) NULL,
    `SyphilisTreatment` VARCHAR(10) NULL,
    `HIVStatusANC` VARCHAR(20) NULL,
    `HIVTestingDone` VARCHAR(20) NULL,
    `HIVTest-1` VARCHAR(20) NULL,
    `HIVTest-1Result` VARCHAR(20) NULL,
    `HIVTest-2` VARCHAR(20) NULL,
    `HIVTestFinalResult` VARCHAR(20) NULL,
    `WHOStaging` VARCHAR(20) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     INDEX patient_patient_id (PatientID),
     INDEX patient_patient_pk (PatientPK),
     INDEX patient_facility_id (FacilityID),
     INDEX patient_site_code (SiteCode),
     INDEX patient_date_created (DateCreated),
     INDEX patient_patient_facility (PatientID,FacilityID)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_all_anc_patients_temp_",queue_number);
                            set @queue_table = concat("ndwr_all_anc_patients_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_all_anc_patients_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_all_anc_patients_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  
                                          
				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_all_anc_patients_temp_",queue_number);
                            
                            SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;
                            
                            set @queue_table = "ndwr_all_ancn_patients_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr.ndwr_all_anc_patients_sync_queue (
                                person_id INT(6) UNSIGNED,
                                INDEX all_patients_sync_person_id (person_id)
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_all_anc_patients_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b WHERE
                   is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL and date_created >= @last_update);
                   
                   replace into ndwr.ndwr_all_anc_patients_sync_queue(
					SELECT 
					DISTINCT PatientID
					FROM
						ndwr.ndwr_all_anc_patients_extract
					WHERE
						DATE(DateCreated) < DATE(DATE_FORMAT(CURDATE(), '%Y-%m-01')));

                  end if;
                  
                  SET @person_ids_count = 0;
				  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
				  PREPARE s1 from @dyn_sql; 
				  EXECUTE s1; 
				  DEALLOCATE PREPARE s1;

                    SELECT @person_ids_count AS 'num patients to build';
                  
                   SELECT CONCAT('Deleting data from ', @primary_table);
                    
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 on (t1.patientid = t2.person_id);'); 
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
							drop  table if exists ndwr_all_anc_patients_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_all_anc_patients_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_all_anc_patients_interim;
                          
CREATE temporary TABLE ndwr_all_anc_patients_interim (
    SELECT
    null as 'PKV',
    t1.person_id AS 'PatientPK',
    mfl.mfl_code AS 'SiteCode',
    t1.person_id AS 'PatientID',
    mfl.mfl_code AS 'FacilityID',
    'AMRS' AS Emr,
    'Ampath Plus' AS 'Project'
   );

                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_all_anc_patients_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_all_anc_patients_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_all_anc_patients_build_queue__0 t2 using (person_id);'); 
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
