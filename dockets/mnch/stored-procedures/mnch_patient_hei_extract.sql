USE ndwr;
DELIMITER $$
CREATE  PROCEDURE `build_ndwr_mnch_patient_hei_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_mnch_patient_hei_extract";
          set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_mnch_patient_hei_extract_v1.0";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_mnch_patient_hei_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_mnch_patient_hei_extract (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `PatientMNCH_ID` VARCHAR(30) NOT NULL,
    `PatientHEI_ID` VARCHAR(30) NULL,
    `1stDNAPCRDate` DATETIME NULL,
    `2ndDNAPCRDate` DATETIME NULL,
    `3rdDNAPCRDate` DATETIME NULL,
    `ConfirmatoryPCRDate` DATETIME NULL,
    `BasellineVLDate` DATETIME NULL,
    `FinalAntibodyDate` DATETIME NULL,
    `1stDNAPCR` VARCHAR(30) NULL,
    `2ndDNAPCR` VARCHAR(30) NULL,
    `3rdDNAPCR` VARCHAR(30) NULL,
    `ConfirmatoryPCR` VARCHAR(30) NULL,
    `BasellineVL` INT NULL,
    `FinalAntibody` VARCHAR(30) NULL,
    `HEIExitDate` DATETIME NULL,
    `HEIHIVStatus` VARCHAR(30) NULL,
    `HEIExitCriteria` VARCHAR(30) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX patient_hei_pk (PatientPK),
    INDEX patient_hei_site_code (SiteCode),
    INDEX patient_hei_mnch_id (PatientMNCH_ID),
    INDEX patient_hei_id (PatientHEI_ID),
    INDEX patient_hei_date_created (DateCreated),
    INDEX patient_patient_hei_site (PatientPK , SiteCode)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_mnch_patient_hei_extract_temp_",queue_number);
                            set @queue_table = concat("ndwr_mnch_patient_hei_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_mnch_patient_hei_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_mnch_patient_hei_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_mnch_patient_hei_extract_temp_",queue_number);
                            
                            SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;
                            
                            set @queue_table = "ndwr_mnch_patient_hei_extract_sync_queue";
CREATE TABLE IF NOT EXISTS ndwr.ndwr_mnch_patient_hei_extract_sync_queue (
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

                            replace into ndwr_mnch_patient_hei_extract_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b WHERE
                   is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL and date_created >= @last_update);
                   
                   replace into ndwr.ndwr_mnch_patient_hei_extract_sync_queue(
					SELECT 
					DISTINCT PatientID
					FROM
						ndwr.ndwr_mnch_patient_hei_extract_extract
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
							drop  table if exists ndwr_mnch_patient_hei_extract_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_mnch_patient_hei_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;

                         

SELECT CONCAT('Creating ndwr_mnch_patient_hei_extract_interim table ...');
                                      
						  
                          drop temporary table if exists ndwr_mnch_patient_hei_extract_interim;
                          
CREATE temporary TABLE ndwr_mnch_patient_hei_extract_interim (
    SELECT
    t1.person_id AS 'PatientPK',
    mfl.mfl_code as 'SiteCode',
    'AMRS' AS Emr,
    'Ampath Plus' AS 'Project',
    mfl.Facility AS FacilityName,
    CASE
        WHEN u.identifier IS NOT NULL THEN  u.identifier
        ELSE an.identifier
    END AS 'PatientMNCH_ID',
    h.identifier as 'PatientHEI_ID',
    t1.hiv_dna_pcr_1_date as '1stDNAPCRDate',
    t1.hiv_dna_pcr_2_date as '2ndDNAPCRDate',
    t1.hiv_dna_pcr_3_date as '3rdDNAPCRDate',
    t1.hiv_dna_pcr_4_date as 'ConfirmatoryPCRDate',
    NULL AS 'BasellineVLDate',
    NULL AS 'FinalyAntibodyDate',
    t1.hiv_dna_pcr_1 as '1stDNAPCR',
    t1.hiv_dna_pcr_2 as '2ndDNAPCR',
    t1.hiv_dna_pcr_3 as '3rdDNAPCR',
    t1.hiv_dna_pcr_4 as 'ConfirmatoryPCR',
    NULL AS 'BasellineVL',
    NULL AS 'FinalyAntibody',
    NULL AS 'HEIExitDate',
    NULL AS 'HEIHIVStatus',
    NULL AS 'HEIExitCriteria',
    NULL AS 'DateCreated'
FROM
    etl.flat_hei_summary t1
    INNER JOIN
    ndwr_mnch_patient_hei_extract_build_queue__0 t3 ON (t3.person_id = t1.person_id)
        JOIN
    ndwr.mfl_codes mfl ON (mfl.location_id = t1.location_id)
        JOIN
    amrs.location l ON (l.location_id = t1.location_id)
    left join 
    amrs.patient_identifier u on (u.patient_id = t1.person_id AND u.identifier_type = 8 AND u.voided = 0)
		left join 
    amrs.patient_identifier an on (an.patient_id = t1.person_id AND an.identifier_type = 3 AND an.voided = 0)
    left join 
    amrs.patient_identifier h on (h.patient_id = t1.person_id AND h.identifier_type = 38 AND h.voided = 0)
    
WHERE
    t1.is_clinical_encounter = 1
        AND t1.next_clinical_datetime_hiv IS NULL);

                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_mnch_patient_hei_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_mnch_patient_hei_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_mnch_patient_hei_extract_build_queue__0 t2 using (person_id);'); 
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
