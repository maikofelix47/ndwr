CREATE  PROCEDURE `build_NDWR_covid_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_covid_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_covid_v1.0";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    -- set @last_date_created = (select max(DateCreated) from ndwr.ndwr_covid_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_covid_extract (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientID` VARCHAR(30) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `FacilityID` INT NULL,
    `VisitID` INT NOT NULL,
    `Covid19AssessmentDate` DATETIME NULL,
    `ReceivedCOVID19Vaccine` VARCHAR(100) NULL,
    `DateGivenFirstDose` DATETIME NOT NULL,
    `FirstDoseVaccineAdministered` VARCHAR(100) NULL,
    `DateGivenSecondDose` DATETIME NULL,
    `SecondDoseVaccineAdministered` VARCHAR(100) NULL,
    `VaccinationStatus` VARCHAR(100) NULL,
    `VaccineVerification` VARCHAR(100) NULL,
    `VaccineVerificationSecondDose` VARCHAR(100) NULL,
    `BoosterGiven` VARCHAR(10) NULL,
    `BoosterDose` INT NULL,
    `Sequence` VARCHAR(50) NULL,
    `COVID19TestResult` VARCHAR(20) NULL,
    `BoosterDoseVerified` VARCHAR(50) NULL,
    `COVID19TestDate` DATETIME NULL,
    `PatientStatus` VARCHAR(50) NULL,
    `AdmissionStatus` VARCHAR(50) NULL,
    `AdmissionUnit` VARCHAR(100) NULL,
    `MissedAppointmentDueToCOVID19` VARCHAR(100) NULL,
    `COVID19PositiveSinceLasVisit` VARCHAR(100) NULL,
    `COVID19TestDateSinceLastVisit` VARCHAR(100) NULL,
    `PatientStatusSinceLastVisit` VARCHAR(100) NULL,
    `AdmissionStatusSinceLastVisit` VARCHAR(100) NULL,
    `AdmissionStartDate` DATETIME NULL,
    `AdmissionEndDate` DATETIME NULL,
    `AdmissionUnitSinceLastVisit` VARCHAR(50) NULL,
    `SupplementalOxygenReceived` VARCHAR(10) NULL,
    `PatientVentilated` VARCHAR(10) NULL,
    `EverCOVID19Positive` VARCHAR(50) NULL,
    `TracingFinalOutcome` VARCHAR(100) NULL,
    `CauseOfDeath` VARCHAR(100) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX patient_covid_patient_id (PatientID),
    INDEX patient_covid_patient_pk (PatientPK),
    INDEX patient_covid_facility_id (FacilityID),
    INDEX patient_covid_site_code (SiteCode),
    INDEX patient_covid_date_created (DateCreated),
    INDEX patient_patient_covid_facility (PatientID , FacilityID)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_covid_extract_temp_",queue_number);
                            set @queue_table = concat("ndwr_covid_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_covid_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_covid_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_covid_temp_",queue_number);
                            
                            SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;
                            
                            set @queue_table = "ndwr_covid_sync_queue";
CREATE TABLE IF NOT EXISTS ndwr.ndwr_covid_sync_queue (
    person_id INT(6) UNSIGNED,
    INDEX covid_sync_person_id (person_id)
);                            
                            
                            set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    ndwr.flat_log
WHERE
    table_name = @table_version;

                            replace into ndwr_covid_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b WHERE
                   is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL and date_created >= @last_update);
                   
                   replace into ndwr.ndwr_covid_sync_queue(
					SELECT 
					DISTINCT PatientID
					FROM
						ndwr.ndwr_covid_extract
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
							drop  table if exists ndwr_covid_extract_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_covid_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;


                         

SELECT CONCAT('Creating ndwr_covid_immunization_flat_obs table ...');
                                      
						    set @boundary := '!!';
                           
                          drop  table if exists ndwr_covid_immunization_flat_obs;
                          
CREATE  TABLE ndwr_covid_immunization_flat_obs (
    SELECT 
    og.person_id,
    og.obs_id,
    og.location_id,
    og.encounter_id,
    e.encounter_datetime,
    og.encounter_id as VisitID,
    e.encounter_datetime as Covid19AssessmentDate,
    og.obs_group_id,
    og.obs_datetime as 'assesment_date',
    og.concept_id,
    og.value_coded,
    GROUP_CONCAT(CASE
            WHEN
                o.value_coded IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_coded,
                        @boundary)
            WHEN
                o.value_numeric IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_numeric,
                        @boundary)
            WHEN
                o.value_datetime IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        DATE(o.value_datetime),
                        @boundary)
            WHEN
                o.value_text IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_text,
                        @boundary)
            WHEN
                o.value_modifier IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_modifier,
                        @boundary)
        END
        ORDER BY o.concept_id , o.value_coded
        SEPARATOR ' ## ') AS obs,
    GROUP_CONCAT(CASE
            WHEN
                o.value_coded IS NOT NULL
                    OR o.value_numeric IS NOT NULL
                    OR o.value_datetime IS NOT NULL
                    OR o.value_text IS NOT NULL
                    OR o.value_drug IS NOT NULL
                    OR o.value_modifier IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        DATE(o.obs_datetime),
                        @boundary)
        END
        ORDER BY o.concept_id , o.value_coded
        SEPARATOR ' ## ') AS obs_datetimes
FROM
    ndwr.ndwr_covid_extract_build_queue__0 q
        JOIN
        amrs.obs og on (og.person_id = q.person_id)
        JOIN
    amrs.obs o ON (og.obs_id = o.obs_group_id)
       join amrs.encounter e on (e.encounter_id = og.encounter_id)
WHERE
        e.encounter_type in (208)
        AND og.concept_id IN (1390)
GROUP BY o.obs_group_id
order by og.obs_datetime
);

SELECT CONCAT('Creating immunization details from immunization flat_obs');

                            SET @ReceivedCOVID19Vaccine:= NULL;
                            SET @DategivenFirstDose:= NULL;
                            SET @FirstDoseVaccineAdministered := NULL;
                            SET @DateGivenSecondDose := NULL;
                            SET @SecondDoseVaccineAdministered := null;
                            set @VaccinationStatus := null;
                            SET @VaccineVerification:= NULL;
                            SET @VaccineVerificationSecondDose := NULL;
                            set @prev_id = -1;
                            set @cur_id = -1;

drop  table if exists ndwr_covid_immunization;
create  table ndwr_covid_immunization(
select c.* from (
select b.* from (
SELECT
 i.*,
 @prev_id := @cur_id as prev_id,
 @cur_id := i.person_id as cur_id,
 i.person_id AS 'PatientPK',
 mfl.mfl_code as 'SiteCode',
 mfl.mfl_code as 'FacilityID',
 mfl.Facility AS 'FacilityName',
 CASE
  WHEN i.obs regexp "!!10485=1!!" THEN 1
  WHEN i.obs regexp "!!10485=2!!" THEN 2
  WHEN i.obs regexp "!!10485=3!!" THEN 3
  else 0
END AS vaccince_sort_index,
CASE
  WHEN i.obs regexp "!!10485=(1|2)!!" THEN @ReceivedCOVID19Vaccine := 1
  when @prev_id = @cur_id then @ReceivedCOVID19Vaccine
  else @ReceivedCOVID19Vaccine := null
END AS 'ReceivedCOVID19Vaccine',
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @DategivenFirstDose := etl.GetValues(i.obs,10958)
  when @prev_id = @cur_id then @DategivenFirstDose
  else @DategivenFirstDose := null
END AS DategivenFirstDose,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @FirstDoseVaccineAdministered := etl.GetValues(i.obs,984)
  when @prev_id = @cur_id then @FirstDoseVaccineAdministered
  else @FirstDoseVaccineAdministered := null
END AS FirstDoseVaccineAdministered,
CASE
  WHEN i.obs regexp "!!10485=2!!" THEN @DateGivenSecondDose:= etl.GetValues(i.obs,10958)
  when @prev_id = @cur_id then @DateGivenSecondDose
  else @DateGivenSecondDose := null
END AS DateGivenSecondDose,
CASE
  WHEN i.obs regexp "!!10485=2!!" THEN @SecondDoseVaccineAdministered := etl.GetValues(i.obs,984)
  when @prev_id = @cur_id then @SecondDoseVaccineAdministered
  else @SecondDoseVaccineAdministered := null
END AS SecondDoseVaccineAdministered,
CASE
  WHEN i.obs regexp "!!2300=" THEN @VaccinationStatus := etl.GetValues(i.obs,2300)
  when @prev_id = @cur_id then @VaccinationStatus
  else @VaccinationStatus := null
END AS VaccinationStatus,
CASE
  WHEN i.obs regexp "!!11906=" THEN @VaccineVerification := etl.GetValues(i.obs,11906)
  when @prev_id = @cur_id then @VaccineVerification
  else @VaccineVerification := null
END AS VaccineVerification,
CASE
  WHEN i.obs regexp "!!11906=" AND i.obs regexp "!!10485=2!!" THEN @VaccineVerificationSecondDose := etl.GetValues(i.obs,11906)
  when @prev_id = @cur_id then @VaccineVerificationSecondDose
  else @VaccineVerificationSecondDose := null
END AS VaccineVerificationSecondDose
FROM
 ndwr_covid_immunization_flat_obs i
  left JOIN
 ndwr.mfl_codes mfl ON (mfl.location_id = i.location_id)

) b order by b.person_id, b.encounter_datetime, b.vaccince_sort_index desc ) c group by c.person_id, encounter_id);

SELECT CONCAT('Creating ndwr_covid_interim table');

drop temporary table if exists ndwr_covid_interim;
create temporary table ndwr_covid_interim(
    SELECT 
     q.person_id as PatientPK,
     i.SiteCode,
     q.person_id as PatientID,
    'AMRS' AS Emr,
    'Ampath Plus' AS 'Project',
     i.FacilityName,
     i.FacilityID,
     i.VisitID,
     i.Covid19AssessmentDate,
     i.ReceivedCOVID19Vaccine,
     i.DateGivenFirstDose,
     i.FirstDoseVaccineAdministered,
     i.DateGivenSecondDose,
     i.SecondDoseVaccineAdministered,
     i.VaccinationStatus,
     i.VaccineVerification,
     i.VaccineVerificationSecondDose,
     NULL AS BoosterGiven,
     null as BoosterDose,
     NULL AS 'Sequence',
     NULL AS COVID19TestResult,
     NULL AS BoosterDoseVerified,
     NULL AS COVID19TestDate,
     NULL AS PatientStatus,
     NULL AS AdmissionStatus,
     NULL AS AdmissionUnit,
     NULL AS MissedAppointmentDueToCOVID19,
     NULL AS COVID19PositiveSinceLasVisit,
     NULL AS COVID19TestDateSinceLastVisit,
     NULL AS PatientStatusSinceLastVisit,
     NULL AS AdmissionStatusSinceLastVisit,
     NULL AS AdmissionStartDate,
     NULL AS AdmissionEndDate,
     NULL AS AdmissionUnitSinceLastVisit,
     NULL AS SupplementalOxygenReceived,
     NULL AS PatientVentilated,
     NULL AS EverCOVID19Positive,
     NULL AS TracingFinalOutcome,
     NULL AS CauseOfDeath,
     NULL AS DateCreated
    FROM
    ndwr.ndwr_covid_extract_build_queue__0 q
    LEFT JOIN 
    ndwr.ndwr_covid_immunization i on (i.person_id = q.person_id)

);


                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_covid_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_covid_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_covid_extract_build_queue__0 t2 using (person_id);'); 
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


END