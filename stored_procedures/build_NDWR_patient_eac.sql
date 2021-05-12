DELIMITER $$
CREATE  PROCEDURE `build_ndwr_patient_eac`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_eac";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_eac_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr.ndwr_patient_eac (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientID` VARCHAR(30) NOT NULL,
    `Emr` VARCHAR(50) NULL,
    `FacilityID` INT NOT NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `VisitID` INT NULL,
    `VisitDate` DATETIME NOT NULL,
    `SessionNumber` INT NULL,
    `FirstSessionDate` DATETIME NULL,
    `PillCountAdherence` INT NULL,
    `MMAS4_1` TINYINT NULL,
    `MMAS4_2` TINYINT NULL,
    `MMAS4_3` TINYINT NULL,
    `MMAS4_4` TINYINT NULL,
    `MMSA8_1` TINYINT NULL,
    `MMSA4_2` TINYINT NULL,
    `MMSA4_3` TINYINT NULL,
    `MMSA4_4` TINYINT NULL,
    `MMSAScore` TINYINT NULL,
    `EACRecievedVL` TINYINT NULL,
    `EACVL` VARCHAR(50) NULL,
    `EACVLConcerns` VARCHAR(200) NULL,
    `EACVLThoughts` VARCHAR(200) NULL,
    `EACWayForward` VARCHAR(200) NULL,
    `EACCognitiveBarrier` VARCHAR(200) NULL,
    `EACBehaviouralBarrier_1` VARCHAR(200) NULL,
    `EACBehaviouralBarrier_2` VARCHAR(200) NULL,
    `EACBehaviouralBarrier_3` VARCHAR(200) NULL,
    `EACBehaviouralBarrier_4` VARCHAR(200) NULL,
    `EACBehaviouralBarrier_5` VARCHAR(200) NULL,
    `EACEmotionalBarriers_1` VARCHAR(200) NULL,
    `EACEmotionalBarriers_2` VARCHAR(20) NULL,
    `EACEconBarrier_1` BOOLEAN NULL,
    `EACEconBarrier_2` VARCHAR(200) NULL,
    `EACEconBarrier_3` VARCHAR(200) NULL,
    `EACEconBarrier_4` BOOLEAN NULL,
    `EACEconBarrier_5` BOOLEAN NULL,
    `EACEconBarrier_6` BOOLEAN NULL,
    `EACEconBarrier_7` BOOLEAN NULL,
    `EACEconBarrier_8` BOOLEAN NULL,
    `EACReviewImprovement` VARCHAR(10) NULL,
    `EACReviewMissedDoses` VARCHAR(10) NULL,
    `EACReviewStrategy` VARCHAR(200) NULL,
    `EACReferral` BOOLEAN NULL,
    `EACReferralApp` BOOLEAN NULL,
    `EACReferralExperience` VARCHAR(200) NULL,
    `EACHomevisit` BOOLEAN NULL,
    `EACAdherencePlan` VARCHAR(50) NULL,
    `EACFollowupDate` VARCHAR(50) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX patient_eac_pk (PatientPK),
    INDEX patient_eac_sc (SiteCode),
    INDEX patient_eac_visit_id (VisitID),
    INDEX patient_eac_pk_site_list (PatientPK , SiteCode),
    INDEX date_created (DateCreated)
);
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_eac);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_patient_eac_temp_",queue_number);
                            set @queue_table = concat("ndwr_patient_eac_build_queue_",queue_number);                    												

										        SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_eac_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_eac_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
				SELECT 
    CONCAT('Deleting patient records in interim ',
            @primary_table);
				                    PREPARE s1 from @dyn_sql; 
				                    EXECUTE s1; 
				                    DEALLOCATE PREPARE s1;  

				              end if;
                      if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_patient_eac_temp_",queue_number);
                            set @queue_table = "ndwr_patient_eac_sync_queue";
CREATE TABLE IF NOT EXISTS ndwr_patient_eac_sync_queue (
    person_id INT PRIMARY KEY
);                            
                            
                            set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    ndwr.flat_log
WHERE
    table_name = @table_version;

                            replace into ndwr_patient_eac_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b where date_created >= @last_update);

                      end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_eac_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_eac_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_patient_eac_1;

                          set @prev_id = -1;
                          set @cur_id = -1;
                          set @first_session_date = null;

SELECT CONCAT('Creating ndwr_patient_eac_1 table');
                          
                         
CREATE temporary TABLE ndwr_patient_eac_1 (
    SELECT 
    @prev_id:=@cur_id AS prev_id,
    @cur_id:=o.person_id AS cur_id,
    o.person_id AS 'PatientPK',
    mfl.mfl_code AS 'SiteCode',
    t.PatientID AS 'PatientID',
    t.Emr AS 'Emr',
    mfl.mfl_code AS 'FacilityID',
    t.Project AS 'Project',
    mfl.Facility AS 'FacilityName',
    o.encounter_id AS 'VisitID',
    o.encounter_datetime AS 'VisitDate',
    CASE
        WHEN
            o.encounter_type IN (110)
        THEN
            CASE
                WHEN o.obs REGEXP '!!(10532)' THEN @mdt_session_number:=4
                WHEN o.obs REGEXP '!!(10527|10528|10529|10530|10531)' THEN @mdt_session_number:=3
                WHEN o.obs REGEXP '!!(10523|10524|10525|10526)' THEN @mdt_session_number:=2
                WHEN o.obs REGEXP '!!(10518|10519|10520|10521|10522)' THEN @mdt_session_number:=1
                WHEN @prev_id = @cur_id THEN @mdt_session_number
                ELSE @mdt_session_number:=NULL
            END
        WHEN @prev_id = @cur_id THEN @mdt_session_number
        ELSE @mdt_session_number:=NULL
    END AS 'SessionNumber',
    CASE
        WHEN
            @mdt_session_number IS NOT NULL
                AND @first_session_date IS NULL
        THEN
            @first_session_date:=o.encounter_datetime
        WHEN @prev_id = @cur_id THEN @first_session_date
        ELSE @first_session_date:=NULL
    END AS 'FirstSessionDate',
    NULL AS 'PillCountAdherence',
    CASE
        WHEN o.obs REGEXP '!!10102=' THEN etl.GetValues(o.obs, 10102)
        ELSE NULL
    END AS 'MMAS4_1',
    CASE
        WHEN o.obs REGEXP '!!10103=' THEN etl.GetValues(o.obs, 10103)
        ELSE NULL
    END AS 'MMAS4_2',
    CASE
        WHEN o.obs REGEXP '!!10104=' THEN etl.GetValues(o.obs, 10104)
        ELSE NULL
    END AS 'MMAS4_3',
    CASE
        WHEN o.obs REGEXP '!!10105=' THEN etl.GetValues(o.obs, 10105)
        ELSE NULL
    END AS 'MMAS4_4',
    CASE
        WHEN o.obs REGEXP '!!10106=' THEN etl.GetValues(o.obs, 10106)
        ELSE NULL
    END AS 'MMSA8_1',
    CASE
        WHEN o.obs REGEXP '!!10107=' THEN etl.GetValues(o.obs, 10107)
        ELSE NULL
    END AS 'MMSA4_2',
    CASE
        WHEN o.obs REGEXP '!!10108=' THEN etl.GetValues(o.obs, 10108)
        ELSE NULL
    END AS 'MMSA4_3',
    CASE
        WHEN o.obs REGEXP '!!10109=' THEN etl.GetValues(o.obs, 10109)
        ELSE NULL
    END AS 'MMSA4_4',
    CASE
        WHEN o.obs REGEXP '!!10111=' THEN etl.GetValues(o.obs, 10111)
        ELSE NULL
    END AS 'MMSAScore',
    NULL AS 'EACRecievedVL',
    NULL AS 'EACVL',
    NULL AS 'EACVLConcerns',
    NULL AS 'EACVLThoughts',
    NULL AS 'EACWayForward',
    CASE
        WHEN o.obs REGEXP '!!10518=' THEN etl.GetValues(o.obs, 10518)
        ELSE NULL
    END AS 'EACCognitiveBarrier',
    NULL AS 'EACBehaviouralBarrier_1',
    CASE
        WHEN o.obs REGEXP '!!10518=' THEN etl.GetValues(o.obs, 10518)
        ELSE NULL
    END AS 'EACBehaviouralBarrier_2',
    CASE
        WHEN o.obs REGEXP '!!10518=' THEN etl.GetValues(o.obs, 10518)
        ELSE NULL
    END AS 'EACBehaviouralBarrier_3',
    CASE
        WHEN o.obs REGEXP '!!10518=' THEN etl.GetValues(o.obs, 10518)
        ELSE NULL
    END AS 'EACBehaviouralBarrier_4',
    CASE
        WHEN o.obs REGEXP '!!10518=' THEN etl.GetValues(o.obs, 10518)
        ELSE NULL
    END AS 'EACBehaviouralBarrier_5',
    CASE
        WHEN o.obs REGEXP '!!10520=' THEN etl.GetValues(o.obs, 10520)
        ELSE NULL
    END AS 'EACEmotionalBarriers_1',
    CASE
        WHEN o.obs REGEXP '!!10520=' THEN etl.GetValues(o.obs, 10520)
        ELSE NULL
    END AS 'EACEmotionalBarriers_2',
    NULL AS 'EACEconBarrier_1',
    CASE
        WHEN o.obs REGEXP '!!10521=' THEN etl.GetValues(o.obs, 10521)
        ELSE NULL
    END AS 'EACEconBarrier_2',
    CASE
        WHEN o.obs REGEXP '!!10521=' THEN etl.GetValues(o.obs, 10521)
        ELSE NULL
    END AS 'EACEconBarrier_3',
    NULL AS 'EACEconBarrier_4',
    NULL AS 'EACEconBarrier_5',
    NULL AS 'EACEconBarrier_6',
    NULL AS 'EACEconBarrier_7',
    NULL AS 'EACEconBarrier_8',
    NULL AS 'EACReviewImprovement',
    NULL AS 'EACReviewMissedDoses',
    CASE
        WHEN o.obs REGEXP '!!10527=' THEN etl.GetValues(o.obs, 10527)
        WHEN o.obs REGEXP '!!10523=' THEN etl.GetValues(o.obs, 10523)
        ELSE NULL
    END AS 'EACReviewStrategy',
    NULL AS 'EACReferral',
    NULL AS 'EACReferralApp',
    NULL AS 'EACReferralExperience',
    NULL AS 'EACHomevisit',
    CASE
        WHEN o.obs REGEXP '!!10530=' THEN etl.GetValues(o.obs, 10530)
        WHEN o.obs REGEXP '!!10526=' THEN etl.GetValues(o.obs, 10526)
        WHEN o.obs REGEXP '!!10522=' THEN etl.GetValues(o.obs, 10522)
        ELSE NULL
    END AS 'EACAdherencePlan',
    NULL AS 'EACFollowupDate'
    FROM
    ndwr.ndwr_patient_eac_build_queue__0 q
        JOIN
    etl.flat_obs o ON (q.person_id = o.person_id)
        JOIN
    ndwr.mfl_codes mfl ON (mfl.location_id = o.location_id)
        JOIN
    ndwr.ndwr_all_patients_extract t ON (t.PatientPK = q.person_id)
WHERE
    o.encounter_type IN (2 , 106, 129, 110)
        AND o.obs REGEXP '!!(10102|10518|10520|10521|10527|10523|10530|10526|10522)='
ORDER BY o.person_id , o.encounter_datetime ASC);

   drop temporary table if exists ndwr_patient_eac_interim;

                          create temporary table ndwr_patient_eac_interim(
 SELECT
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    FacilityID,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    SessionNumber,
    FirstSessionDate,
    PillCountAdherence,
    MMAS4_1,
    MMAS4_2,
    MMAS4_3,
    MMAS4_4,
    MMSA8_1,
    MMSA4_2,
    MMSA4_3,
    MMSA4_4,
    MMSAScore,
    EACRecievedVL,
    EACVL,
    EACVLConcerns,
    EACVLThoughts,
    EACWayForward,
    EACCognitiveBarrier,
    EACBehaviouralBarrier_1,
    EACBehaviouralBarrier_2,
    EACBehaviouralBarrier_3,
    EACBehaviouralBarrier_4,
    EACBehaviouralBarrier_5,
    EACEmotionalBarriers_1,
    EACEmotionalBarriers_2,
    EACEconBarrier_1,
    EACEconBarrier_2,
    EACEconBarrier_3,
    EACEconBarrier_4,
    EACEconBarrier_5,
    EACEconBarrier_6,
    EACEconBarrier_7,
    EACEconBarrier_8,
    EACReviewImprovement,
    EACReviewMissedDoses,
    EACReviewStrategy,
    EACReferral,
    EACReferralApp,
    EACReferralExperience,
    EACHomevisit,
    EACAdherencePlan,
    EACFollowupDate,
    NULL as 'DateCreated'
  FROM
  ndwr_patient_eac_1);
                          
						 

                         

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_eac_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_eac_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_eac_build_queue__0 t2 using (person_id);'); 
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
