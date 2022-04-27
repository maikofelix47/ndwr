DELIMITER $$
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
                    set @boundary := '!!';

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
    `NextCovidAssessmentDate` DATETIME NULL,
    `ReceivedCOVID19Vaccine` VARCHAR(100) NULL,
    `DateGivenFirstDose` DATETIME NULL,
    `FirstDoseVaccineAdministered` VARCHAR(100) NULL,
    `DateGivenSecondDose` DATETIME NULL,
    `SecondDoseVaccineAdministered` VARCHAR(100) NULL,
    `VaccinationStatus` VARCHAR(100) NULL,
    `VaccineVerification` VARCHAR(100) NULL,
    `VaccineVerificationSecondDose` VARCHAR(100) NULL,
    `BoosterGiven` VARCHAR(10) NULL,
    `BoosterVaccine` VARCHAR(30) NULL,
    `BoosterDoseDate` DATETIME NULL,
    `BoosterDose` INT NULL,
    `BoosterDoseVerified` VARCHAR(50) NULL,
    `Sequence` VARCHAR(50) NULL,
    `COVID19TestResult` VARCHAR(20) NULL,
    `COVID19TestDate` DATETIME NULL,
    `PatientStatus` VARCHAR(50) NULL,
    `HospitalAdmission` VARCHAR(50) NULL,
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
                  
SELECT 
    CONCAT('Deleting test patients from ',
            @queue_table);
                  
                  
                   SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
                            join amrs.person_attribute t2 using (person_id)
                            where t2.person_attribute_type_id=28 and value="true" and voided=0');
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    
                  
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


                         ## create covid_screenings temporary table
                         drop table if exists ndwr.ndwr_covid_encounters_temp;
CREATE TABLE ndwr_covid_encounters_temp (SELECT o.patient_id AS person_id,
    o.encounter_id,
    o.encounter_datetime,
    o.location_id FROM
    ndwr.ndwr_covid_extract_build_queue__0 q
        JOIN
    amrs.encounter o ON (o.patient_id = q.person_id)
WHERE
    o.encounter_type IN (208)
    and o.encounter_datetime >= '2022-04-01 00:00:00'
        AND o.voided = 0);
                         
SELECT CONCAT('Creating flat_covid_immunization_obs_test table');
drop temporary table if exists ndwr.flat_covid_obs_test;
create temporary table flat_covid_obs_test(
SELECT 
    og.person_id,
    t.encounter_id,
    t.encounter_datetime,
    og.obs_id as 'obs_group_id',
    o.obs_id,
    o.concept_id,
    o.value_coded,
    o.value_datetime,
    o.value_numeric,
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
    ndwr_covid_encounters_temp t
        JOIN
    amrs.obs og ON (t.encounter_id = og.encounter_id
        AND og.voided = 0)
   join amrs.obs o on (o.obs_group_id = og.obs_id)
WHERE
        og.concept_id = 1390
		AND og.voided = 0
        AND o.voided = 0
GROUP BY og.obs_id
ORDER BY og.obs_datetime asc, og.encounter_id);


SELECT CONCAT('Creating ndwr_immunization_data');

                            SET @DategivenFirstDose:= NULL;
                            SET @FirstDoseVaccineAdministered := NULL;
                            SET @DateGivenSecondDose := NULL;
                            SET @SecondDoseVaccineAdministered := null;
                            set @VaccinationStatus := null;
                            SET @VaccineVerification:= NULL;
                            SET @VaccineVerificationSecondDose := NULL;
                            set @prev_id = -1;
                            set @cur_id = -1;

drop  table if exists ndwr.ndwr_immunization_data;
CREATE TABLE ndwr.ndwr_immunization_data (SELECT i.*,
    @prev_id:=@cur_id AS prev_id,
    @cur_id:=i.person_id AS cur_id,
    CASE
        WHEN i.obs REGEXP '!!10485=1!!' THEN 1
        WHEN i.obs REGEXP '!!10485=2!!' THEN 2
        WHEN i.obs REGEXP '!!10485=3!!' THEN 3
        ELSE 0
    END AS vaccince_sort_index,
    CASE
        WHEN i.obs REGEXP '!!10485=1!!' THEN @DategivenFirstDose:=etl.GetValues(i.obs, 10958)
        WHEN @prev_id = @cur_id THEN @DategivenFirstDose
        ELSE @DategivenFirstDose:=NULL
    END AS DategivenFirstDose,
    CASE
        WHEN i.obs REGEXP '!!10485=1!!' THEN @FirstDoseVaccineAdministered:=etl.GetValues(i.obs, 984)
        WHEN @prev_id = @cur_id THEN @FirstDoseVaccineAdministered
        ELSE @FirstDoseVaccineAdministered:=NULL
    END AS FirstDoseVaccineAdministered,
    CASE
        WHEN i.obs REGEXP '!!10485=2!!' THEN @DateGivenSecondDose:=etl.GetValues(i.obs, 10958)
        WHEN @prev_id = @cur_id THEN @DateGivenSecondDose
        ELSE @DateGivenSecondDose:=NULL
    END AS DateGivenSecondDose,
    CASE
        WHEN i.obs REGEXP '!!10485=2!!' THEN @SecondDoseVaccineAdministered:=etl.GetValues(i.obs, 984)
        WHEN @prev_id = @cur_id THEN @SecondDoseVaccineAdministered
        ELSE @SecondDoseVaccineAdministered:=NULL
    END AS SecondDoseVaccineAdministered,
    CASE
        WHEN i.obs REGEXP '!!2300=' THEN @VaccinationStatus:=etl.GetValues(i.obs, 2300)
        WHEN @prev_id = @cur_id THEN @VaccinationStatus
        ELSE @VaccinationStatus:=NULL
    END AS VaccinationStatus,
    CASE
        WHEN i.obs REGEXP '!!11906=' THEN @VaccineVerification:=etl.GetValues(i.obs, 11906)
        WHEN @prev_id = @cur_id THEN @VaccineVerification
        ELSE @VaccineVerification:=NULL
    END AS VaccineVerification,
    CASE
        WHEN
            i.obs REGEXP '!!11906='
                AND i.obs REGEXP '!!10485=2!!'
        THEN
            @VaccineVerificationSecondDose:=etl.GetValues(i.obs, 11906)
        WHEN @prev_id = @cur_id THEN @VaccineVerificationSecondDose
        ELSE @VaccineVerificationSecondDose:=NULL
    END AS VaccineVerificationSecondDose FROM
    flat_covid_obs_test i
ORDER BY i.person_id ,i.encounter_datetime asc);
 
SELECT CONCAT('Creating ndwr_vaccination_encounter_summary table');
 drop  table if exists ndwr.ndwr_vaccination_encounter_summary;
 create  table ndwr.ndwr_vaccination_encounter_summary(
 SELECT 
    t.*,
    fd.DateGivenFirstDose,
	fd.FirstDoseVaccineAdministered,
	sd.DateGivenSecondDose,
	sd.SecondDoseVaccineAdministered,
    CASE
      WHEN sd.VaccinationStatus IS NOT NULL THEN sd.VaccinationStatus
      WHEN fd.VaccinationStatus IS NOT NULL  AND  sd.VaccinationStatus is null THEN fd.VaccinationStatus
      ELSE sd.VaccinationStatus
    END AS VaccinationStatus,
     CASE
      WHEN sd.VaccineVerification IS NOT NULL THEN sd.VaccineVerification
      WHEN fd.VaccineVerification IS NOT NULL AND sd.VaccineVerification is null THEN fd.VaccineVerification
      ELSE sd.VaccineVerification
    END AS VaccineVerification,
	sd.VaccineVerificationSecondDose
FROM
    ndwr.ndwr_covid_encounters_temp t
        LEFT JOIN
    ndwr.ndwr_immunization_data fd ON (t.person_id = fd.person_id
        AND t.encounter_id = fd.encounter_id
        AND fd.vaccince_sort_index = 1)
        LEFT JOIN
    ndwr.ndwr_immunization_data sd ON (t.person_id = sd.person_id
        AND t.encounter_id = sd.encounter_id
        AND sd.vaccince_sort_index = 2)
        group by t.person_id, t.encounter_id
 );
 
 
SELECT CONCAT('Creating flat_covid_booster_obs_test table');

drop temporary table if exists ndwr.flat_covid_booster_obs_test;
create temporary table ndwr.flat_covid_booster_obs_test(
SELECT 
    og.person_id,
    t.encounter_id,
    t.encounter_datetime,
    og.obs_id as 'obs_group_id',
    o.obs_id,
    o.concept_id,
    o.value_coded,
    o.value_datetime,
    o.value_numeric,
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
    ndwr_covid_encounters_temp t
        JOIN
    amrs.obs og ON (t.encounter_id = og.encounter_id
        AND og.voided = 0)
   join amrs.obs o on (o.obs_group_id = og.obs_id)
WHERE
        og.concept_id = 1944
        AND og.voided = 0
        AND o.voided = 0
GROUP BY og.obs_id
ORDER BY og.obs_datetime asc, og.encounter_id);

SELECT CONCAT('Creating ndwr_booster_data');


 SET @BoosterVaccine:= NULL;
 SET @BoosterDoseDate := NULL;
 SET @BoosterDose := NULL;
 SET @BoosterDoseVerified := null;
 set @prev_id = -1;
 set @cur_id = -1;

drop temporary table if exists ndwr.ndwr_booster_data;
create temporary table ndwr.ndwr_booster_data(
select
i.*,
 @prev_id := @cur_id as prev_id,
 @cur_id := i.person_id as cur_id,
 CASE
  WHEN i.obs regexp "!!10485=1!!" THEN 1
  WHEN i.obs regexp "!!10485=2!!" THEN 2
  WHEN i.obs regexp "!!10485=3!!" THEN 3
  else 0
END AS booster_sort_index,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @BoosterVaccine := etl.GetValues(i.obs,984)
  when @prev_id = @cur_id then @BoosterVaccine
  else @BoosterVaccine := null
END AS BoosterVaccine,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @BoosterDoseDate := etl.GetValues(i.obs,10958)
  when @prev_id = @cur_id then @BoosterDoseDate
  else @BoosterDoseDate := null
END AS BoosterDoseDate,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @BoosterDose := etl.GetValues(i.obs,10485)
  when @prev_id = @cur_id then @BoosterDose
  else @BoosterDose:= null
END AS BoosterDose,
CASE
  WHEN i.obs regexp "!!11906=" THEN @BoosterDoseVerified := etl.GetValues(i.obs,11906)
  when @prev_id = @cur_id then @BoosterDoseVerified
  else @BoosterDoseVerified:= null
END AS BoosterDoseVerified

FROM
 flat_covid_booster_obs_test i
 order by i.encounter_datetime, booster_sort_index asc);
 
SELECT CONCAT('Creating ndwr_booster_encounter_summary table');
 drop temporary table if exists ndwr.ndwr_booster_encounter_summary;
 create temporary table ndwr_booster_encounter_summary(
 SELECT 
    t.*,
    fd.BoosterVaccine,
    fd.BoosterDoseDate,
	fd.BoosterDose,
	fd.BoosterDoseVerified
FROM
    ndwr.ndwr_covid_encounters_temp t
        LEFT JOIN
    ndwr.ndwr_booster_data fd ON (t.person_id = fd.person_id
        AND t.encounter_id = fd.encounter_id
        AND fd.booster_sort_index = 1)
        group by t.person_id, t.encounter_id
 );
 
 
SELECT CONCAT('Creating ndwr_covid_screening_data');
 
 set @ReceivedCOVID19Vaccine:= null;
 set @COVID19TestEver:= null;
 set @COVID19TestResult:= null;
 set @COVID19TestDate:= null;
 set @COVID19Presentation:= null;
 set @HospitalAdmission:= null;
 set @AdmissionUnit:= null;
 set @COVID19PositiveSinceLasVisit:= null;
 set @COVID19TestDateSinceLastVisit:= null;
 set @COVID19PresentationSinceLastVisit:= null;
 set @AdmissionStatusSinceLastVisit:= null;
 set @AdmissionStartDate:= null;
 set @AdmissionEndDate:= null;
 set @AdmissionUnitSinceLastVisit:= null;
 set @SupplementalOxygenReceived:= null;
 set @BoosterGiven:= null;
 set @prev_id = -1;
 set @cur_id = -1;
 
  drop temporary table if exists ndwr.ndwr_covid_screening_data;
 create temporary table ndwr.ndwr_covid_screening_data(
    select 
    t.*,
	@prev_id := @cur_id as prev_id,
	@cur_id := t.person_id as cur_id,
    CASE
	  WHEN o.obs regexp "!!11899=" THEN @ReceivedCOVID19Vaccine := etl.GetValues(o.obs,11899)
      WHEN o.obs regexp "!!10485=(1|2)!!" THEN @ReceivedCOVID19Vaccine := 1065
	  when @prev_id = @cur_id then @ReceivedCOVID19Vaccine
	  else @ReceivedCOVID19Vaccine := null
	END AS ReceivedCOVID19Vaccine,
    CASE
	  WHEN o.obs regexp "!!11911=" THEN @BoosterGiven := etl.GetValues(o.obs,11911)
      when @prev_id = @cur_id then @BoosterGiven
	  else @BoosterGiven := null
	END AS BoosterGiven,
	CASE
	  WHEN o.obs regexp "!!11909=" THEN @COVID19TestEver := etl.GetValues(o.obs,11909)
	  when @prev_id = @cur_id then @COVID19TestEver
	  else @COVID19TestEver := null
	END AS 'COVID19TestEver',
    CASE
	  WHEN o.obs regexp "!!11909=" THEN @COVID19TestResult := etl.GetValues(o.obs,11908)
	  when @prev_id = @cur_id then @COVID19TestResult
	  else @COVID19TestResult := null
	END AS 'COVID19TestResult',
    CASE
	  WHEN o.obs regexp "!!9728=" THEN @COVID19TestDate := etl.GetValues(o.obs,9728)
	  when @prev_id = @cur_id then @COVID19TestDate
	  else @COVID19TestDate := null
	END AS 'COVID19TestDate',
    CASE
	  WHEN o.obs regexp "!!11124=" THEN @COVID19Presentation := etl.GetValues(o.obs,11124)
      when @prev_id = @cur_id then @COVID19Presentation
	  else @COVID19Presentation := null
	END AS 'COVID19Presentation',
	CASE
	  WHEN o.obs regexp "!!6419=" THEN @HospitalAdmission := etl.GetValues(o.obs,6419)
	  when @prev_id = @cur_id then @HospitalAdmission
	  else @HospitalAdmission := null
	END AS 'HospitalAdmission',
    CASE
	  WHEN o.obs regexp "!!11912=" THEN @AdmissionUnit := etl.GetValues(o.obs,11912)
	  when @prev_id = @cur_id then @AdmissionUnit
	  else @AdmissionUnit := null
	END AS 'AdmissionUnit',
    CASE
	  WHEN o.obs regexp "!!11916=" THEN @SupplementalOxygenReceived := etl.GetValues(o.obs,11916)
      when @prev_id = @cur_id then @SupplementalOxygenReceived
	  else @SupplementalOxygenReceived := null
	END AS 'SupplementalOxygenReceived'
    from  
    ndwr.ndwr_covid_encounters_temp t
    join etl.flat_obs o on (o.person_id = t.person_id AND t.encounter_id = o.encounter_id)
    order by t.person_id,t.encounter_datetime asc
 
 );
 
 
 
 
 
 
 
                         
SELECT CONCAT('Creating ndwr_covid_immunization_booster_screening table');

drop temporary table if exists ndwr_covid_immunization_booster_screening;
create temporary table ndwr_covid_immunization_booster_screening(
    SELECT 
     q.person_id as PatientPK,
     mfl.mfl_code AS SiteCode,
     q.person_id as PatientID,
    'AMRS' AS Emr,
    'Ampath Plus' AS 'Project',
     mfl.Facility AS FacilityName,
     mfl.mfl_code AS FacilityID,
     t.encounter_id AS VisitID,
     t.encounter_datetime AS Covid19AssessmentDate,
     s.ReceivedCOVID19Vaccine,
     v.DateGivenFirstDose,
     v.FirstDoseVaccineAdministered,
     v.DateGivenSecondDose,
     v.SecondDoseVaccineAdministered,
     v.VaccinationStatus,
     v.VaccineVerification,
     v.VaccineVerificationSecondDose,
     s.BoosterGiven,
     b.BoosterVaccine,
     b.BoosterDoseDate,
     b.BoosterDose,
     NULL AS 'Sequence',
     s.COVID19TestResult,
     b.BoosterDoseVerified,
     s.COVID19TestDate,
     NULL AS PatientStatus,
     s.HospitalAdmission,
     s.AdmissionUnit,
     NULL AS MissedAppointmentDueToCOVID19,
     NULL AS COVID19PositiveSinceLasVisit,
     NULL AS COVID19TestDateSinceLastVisit,
     NULL AS PatientStatusSinceLastVisit,
     NULL AS AdmissionStatusSinceLastVisit,
     NULL AS AdmissionStartDate,
     NULL AS AdmissionEndDate,
     NULL AS AdmissionUnitSinceLastVisit,
     s.SupplementalOxygenReceived,
     NULL AS PatientVentilated,
     NULL AS EverCOVID19Positive,
     NULL AS TracingFinalOutcome,
     NULL AS CauseOfDeath
    FROM
    ndwr.ndwr_covid_extract_build_queue__0 q
    join ndwr.ndwr_covid_encounters_temp t on (t.person_id = q.person_id)
    left join ndwr_vaccination_encounter_summary v on (v.person_id = q.person_id AND v.encounter_id = t.encounter_id)
    left join ndwr_booster_encounter_summary b on (b.person_id = q.person_id AND b.encounter_id = t.encounter_id)
    left join ndwr.ndwr_covid_screening_data s on (s.person_id = q.person_id AND s.encounter_id = t.encounter_id)
     left JOIN
   ndwr.mfl_codes mfl ON (mfl.location_id = t.location_id)

);


SELECT CONCAT('Creating ndwr_covid_next');

 set @prev_id = -1;
 set @cur_id = -1;
 set @prev_screening_date = null;
 set @cur_screening_date = null;
 set @cur_screening_date = null;

drop temporary table if exists ndwr.ndwr_covid_next;
create temporary table ndwr_covid_next(
select 
@prev_id := @cur_id as prev_id,
@cur_id := t.PatientPK as cur_id,
 case
	when @prev_id = @cur_id then @prev_screening_date := @cur_screening_date
	else @prev_screening_date := null
end as NextCovid19AssessmentDate,
@cur_screening_date := t.Covid19AssessmentDate as CurCovid19AssessmentDate,
t.* 
from 
ndwr_covid_immunization_booster_screening t 
order by t.Covid19AssessmentDate DESC
);

SET @DateGivenFirstDose = NULL;
SET @FirstDoseVaccineAdministered:= null;
SET @DateGivenSecondDose := NULL;
SET @SecondDoseVaccineAdministered := null;
set @VaccinationStatus:= null;
set @VaccineVerification:= null;
set @BoosterGiven:= null;
set @BoosterVaccine:= null;
set @BoosterDoseDate:= null;
set @BoosterDose:= null;
set @BoosterDoseVerified:= null;
set @prev_id = -1;
set @cur_id = -1;

alter table ndwr.ndwr_covid_next drop column prev_id, drop column cur_id;

SELECT CONCAT('Creating lag table');
drop temporary table if exists ndwr.ndwr_covid_lag;
create temporary table ndwr.ndwr_covid_lag(
SELECT 
    @prev_id:=@cur_id AS prev_id,
    @cur_id:=e.PatientPK AS cur_id,
    CASE
        WHEN
            @DateGivenFirstDose IS NULL
                AND @prev_id != @cur_id
                AND e.DateGivenFirstDose IS NOT NULL
        THEN
            @DateGivenFirstDose:=e.DateGivenFirstDose
		WHEN
            @DateGivenFirstDose IS NULL
                AND @prev_id = @cur_id
                AND e.DateGivenFirstDose IS NOT NULL
        THEN
            @DateGivenFirstDose:=e.DateGivenFirstDose
        WHEN @prev_id = @cur_id THEN @DateGivenFirstDose
        ELSE @DateGivenFirstDose:=NULL
    END AS DateGivenFirstDoseLag,
    CASE
        WHEN
            @FirstDoseVaccineAdministered IS NULL
                AND @prev_id != @cur_id
                AND e.FirstDoseVaccineAdministered IS NOT NULL
        THEN
            @FirstDoseVaccineAdministered:=e.FirstDoseVaccineAdministered
		 WHEN
            @FirstDoseVaccineAdministered IS NULL
                AND @prev_id = @cur_id
                AND e.FirstDoseVaccineAdministered IS NOT NULL
        THEN
            @FirstDoseVaccineAdministered:=e.FirstDoseVaccineAdministered
        WHEN @prev_id = @cur_id THEN @FirstDoseVaccineAdministered
        ELSE @FirstDoseVaccineAdministered:=NULL
    END AS FirstDoseVaccineAdministeredLag,
    CASE
        WHEN
            @DateGivenSecondDose IS NULL
                AND @prev_id != @cur_id
                AND e.DateGivenSecondDose IS NOT NULL
        THEN
            @DateGivenSecondDose:=e.DateGivenSecondDose
		WHEN
            @DateGivenSecondDose IS NULL
                AND @prev_id = @cur_id
                AND e.DateGivenSecondDose IS NOT NULL
        THEN
            @DateGivenSecondDose:=e.DateGivenSecondDose
        WHEN @prev_id = @cur_id THEN @DateGivenSecondDose
        ELSE @DateGivenSecondDose:=NULL
    END AS DateGivenSecondDoseLag,
    CASE
        WHEN
            @SecondDoseVaccineAdministered IS NULL
                AND @prev_id != @cur_id
                AND e.SecondDoseVaccineAdministered IS NOT NULL
        THEN
            @SecondDoseVaccineAdministered:=e.SecondDoseVaccineAdministered
		 WHEN
            @SecondDoseVaccineAdministered IS NULL
                AND @prev_id = @cur_id
                AND e.SecondDoseVaccineAdministered IS NOT NULL
        THEN
            @SecondDoseVaccineAdministered:=e.SecondDoseVaccineAdministered
        WHEN @prev_id = @cur_id THEN @SecondDoseVaccineAdministered
        ELSE @SecondDoseVaccineAdministered:=NULL
    END AS SecondDoseVaccineAdministeredLag,
    CASE
        WHEN
            @VaccinationStatus IS NULL
                AND @prev_id != @cur_id
                AND e.VaccinationStatus IS NOT NULL
        THEN
            @VaccinationStatus:=e.VaccinationStatus
		 WHEN
            @VaccinationStatus IS NULL
                AND @prev_id = @cur_id
                AND e.VaccinationStatus IS NOT NULL
        THEN
            @VaccinationStatus:=e.VaccinationStatus
		WHEN @VaccinationStatus is NOT NULL AND e.VaccinationStatus = 2208 THEN @VaccinationStatus:=e.VaccinationStatus
        WHEN @prev_id = @cur_id THEN @VaccinationStatus
        ELSE @VaccinationStatus:=NULL
    END AS VaccinationStatusLag,
    CASE
        WHEN
            @VaccineVerification IS NULL
                AND @prev_id != @cur_id
                AND e.VaccineVerification IS NOT NULL
        THEN
            @VaccineVerification:=e.VaccineVerification
		 WHEN
            @VaccineVerification IS NULL
                AND @prev_id = @cur_id
                AND e.VaccineVerification IS NOT NULL
        THEN
            @VaccineVerification := e.VaccineVerification
		WHEN @VaccineVerification is NOT NULL AND e.VaccineVerification = 2300 THEN @VaccineVerification := e.VaccineVerification
        WHEN @prev_id = @cur_id THEN @VaccineVerification
        ELSE @VaccineVerification:=NULL
    END AS VaccineVerificationLag,
    CASE
        WHEN
            @BoosterGiven IS NULL
                AND @prev_id != @cur_id
                AND e.BoosterGiven IS NOT NULL
        THEN
            @BoosterGiven:=e.BoosterGiven
		 WHEN
            @BoosterGiven IS NULL
                AND @prev_id = @cur_id
                AND e.BoosterGiven IS NOT NULL
        THEN
            @BoosterGiven:=e.BoosterGiven
		WHEN @BoosterGiven is NOT NULL AND e.BoosterGiven = 1065 THEN @BoosterGiven:=e.BoosterGiven
        WHEN @prev_id = @cur_id THEN @BoosterGiven
        ELSE @BoosterGiven:=NULL
    END AS BoosterGivenLag,
    CASE
        WHEN
            @BoosterVaccine IS NULL
                AND @prev_id != @cur_id
                AND e.BoosterVaccine IS NOT NULL
        THEN
            @BoosterVaccine:=e.BoosterVaccine
		 WHEN
            @BoosterVaccine IS NULL
                AND @prev_id = @cur_id
                AND e.BoosterVaccine IS NOT NULL
        THEN
            @BoosterVaccine:=e.BoosterVaccine
        WHEN @prev_id = @cur_id THEN @BoosterVaccine
        ELSE @BoosterVaccine:=NULL
    END AS BoosterVaccineLag,
    CASE
        WHEN
            @BoosterDoseDate IS NULL
                AND @prev_id != @cur_id
                AND e.BoosterDoseDate IS NOT NULL
        THEN
            @BoosterDoseDate:=e.BoosterDoseDate
		WHEN
            @BoosterDoseDate IS NULL
                AND @prev_id = @cur_id
                AND e.BoosterDoseDate IS NOT NULL
        THEN
            @BoosterDoseDate:=e.BoosterDoseDate
        WHEN @prev_id = @cur_id THEN @BoosterDoseDate
        ELSE @BoosterDoseDate:=NULL
    END AS BoosterDoseDateLag,
    CASE
        WHEN
            @BoosterDose IS NULL
                AND @prev_id != @cur_id
                AND e.BoosterDose IS NOT NULL
        THEN
            @BoosterDose:=e.BoosterDose
		WHEN
            @BoosterDose IS NULL
                AND @prev_id = @cur_id
                AND e.BoosterDose IS NOT NULL
        THEN
            @BoosterDose:=e.BoosterDose
        WHEN @prev_id = @cur_id THEN @BoosterDose
        ELSE @BoosterDose:=NULL
    END AS BoosterDoseLag,
    CASE
        WHEN
            @BoosterDoseVerified IS NULL
                AND @prev_id != @cur_id
                AND e.BoosterDoseVerified IS NOT NULL
        THEN
            @BoosterDoseVerified:=e.BoosterDoseVerified
		WHEN
            @BoosterDoseVerified IS NULL
                AND @prev_id = @cur_id
                AND e.BoosterDoseVerified IS NOT NULL
        THEN
            @BoosterDoseVerified:=e.BoosterDoseVerified
        WHEN @prev_id = @cur_id THEN @BoosterDoseVerified
        ELSE @BoosterDoseVerified:=NULL
    END AS BoosterDoseVerifiedLag,
    e.*
FROM
   ndwr.ndwr_covid_next e
ORDER BY e.PatientPK,e.Covid19AssessmentDate ASC
);


SELECT CONCAT('Creating interim table');
drop temporary table if exists ndwr.ndwr_covid_interim;
create temporary table ndwr.ndwr_covid_interim(
select
	 PatientPK,
     SiteCode,
     PatientID,
     Emr,
     Project,
     FacilityName,
     FacilityID,
     VisitID,
     Covid19AssessmentDate,
     NextCovid19AssessmentDate,
     ReceivedCOVID19Vaccine,
     DateGivenFirstDoseLag as DateGivenFirstDose,
     FirstDoseVaccineAdministeredLag as FirstDoseVaccineAdministered,
     DateGivenSecondDoseLag as DateGivenSecondDose,
     SecondDoseVaccineAdministeredLag as SecondDoseVaccineAdministered,
     VaccinationStatusLag as VaccinationStatus,
     VaccineVerificationLag as VaccineVerification,
     VaccineVerificationSecondDose,
     BoosterGivenLag as BoosterGiven,
     BoosterVaccineLag as BoosterVaccine,
     BoosterDoseDateLag as BoosterDoseDate,
     BoosterDoseLag as BoosterDose,
     BoosterDoseVerifiedLag as BoosterDoseVerified,
     Sequence,
     COVID19TestResult,
     COVID19TestDate,
     PatientStatus,
     HospitalAdmission,
     AdmissionUnit,
     MissedAppointmentDueToCOVID19,
     COVID19PositiveSinceLasVisit,
     COVID19TestDateSinceLastVisit,
     PatientStatusSinceLastVisit,
     AdmissionStatusSinceLastVisit,
     AdmissionStartDate,
     AdmissionEndDate,
     AdmissionUnitSinceLastVisit,
     SupplementalOxygenReceived,
     PatientVentilated,
     EverCOVID19Positive,
     TracingFinalOutcome,
     CauseOfDeath,
     NULL AS DateCreated
     from ndwr.ndwr_covid_lag
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
                          
                          
						  SET @dyn_sql=CONCAT('drop table ndwr.ndwr_immunization_data;'); 
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
