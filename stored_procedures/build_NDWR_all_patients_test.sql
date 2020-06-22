DELIMITER $$
CREATE  PROCEDURE `build_NDWR_all_patients_test`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN end_date varchar(50) ,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_all_patients_extract_test";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_all_patients_v1.0";
                    set @query_type=query_type;
                    set @end_date = end_date;

CREATE TABLE IF NOT EXISTS ndwr_all_patients_extract_test (
    `PatientID` INT NOT NULL,
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `FacilityName` VARCHAR(100) NULL,
    `Gender` VARCHAR(10) NULL,
    `DOB` DATETIME NULL,
    `RegistrationDate` DATETIME NOT NULL,
    `RegistrationAtCCC` DATETIME NOT NULL,
    `RegistrationAtPMTCT` DATETIME NULL,
    `RegistrationAtTBClinic` DATETIME NULL,
    `PatientSource` VARCHAR(100) NULL,
    `Region` VARCHAR(100) NULL,
    `District` VARCHAR(100) NULL,
    `Village` VARCHAR(100) NULL,
    `ContactRelation` VARCHAR(250) NULL,
    `LastVisit` DATETIME NULL,
    `MaritalStatus` VARCHAR(100) NULL,
    `EducationLevel` VARCHAR(50) NULL,
    `DateConfirmedHIVPositive` DATETIME NULL,
    `PreviousARTExposure` VARCHAR(50) NULL,
    `PreviousARTStartDate` DATETIME NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityID` INT NULL,
    `StatusAtCCC` VARCHAR(100) NULL,
    `StatusAtPMTCT` VARCHAR(100) NULL,
    `StatusAtTBClinic` VARCHAR(100) NULL,
    `SatelliteName` VARCHAR(100) NULL,
    `arv_first_regimen_start_date` DATE NULL,
    `rtc_date` DATE NULL,
    `arv_first_regimen` VARCHAR(200) NULL,
    `arv_start_date` DATE NULL,
    `cur_arv_meds` VARCHAR(200) NULL,
    `cur_arv_line_strict` VARCHAR(250) NULL,
    `Inschool` VARCHAR(100) NULL,
    `KeyPopulationType` VARCHAR(100) NULL,
    `Orphan` VARCHAR(100) NULL,
    `PatientResidentCounty` VARCHAR(100) NULL,
    `PatientResidentLocation` VARCHAR(100) NULL,
    `PatientResidentSubCounty` VARCHAR(100) NULL,
    `PatientResidentSubLocation` VARCHAR(100) NULL,
    `PatientResidentVillage` VARCHAR(100) NULL,
    `PatientResidentWard` VARCHAR(100) NULL,
    `PatientType` VARCHAR(100) NULL,
    `PopulationType` VARCHAR(100) NULL,
    `TransferInDate` DATETIME NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     INDEX patient_patient_id (PatientID),
     INDEX patient_patient_pk (PatientPK),
     INDEX patient_facility_id (FacilityID),
     INDEX patient_site_code (SiteCode),
     INDEX patient_date_created (DateCreated),
     INDEX patient_patient_facility (PatientID,FacilityID)
     INDEX patient_rtc (PatientID,rtc_date),
     INDEX patient_reg_start (PatientID,arv_first_regimen_start_date)
     INDEX patient_arv_start (PatientID,arv_first_regimen_start_date)
     INDEX patient_transfer_in (PatientID,arv_start_date)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_all_patients_test_temp_",queue_number);
                            set @queue_table = concat("ndwr_all_patients_test_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_all_patients_test_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_all_patients_test_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  
                                          
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
							drop  table if exists ndwr_all_patients_test_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_all_patients_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_all_patients_test_interim;
                          
CREATE temporary TABLE ndwr_all_patients_test_interim (SELECT 
    DISTINCT t1.person_id AS PatientID,
    t1.person_id AS PatientPK,
    mfl.mfl_code as SiteCode,
    mfl.Facility AS FacilityName,             
    gender AS Gender,
    birthdate AS DOB,
    CASE
        WHEN
            DATE(t1.enrollment_date) = '1900-01-01'
                AND DATE(birthdate) <= '1997-01-01'
        THEN
            '1997-01-01'
        WHEN
            DATE(t1.enrollment_date) = '1900-01-01'
                AND DATE(birthdate) > '1997-01-01'
        THEN
            DATE_ADD(birthdate, INTERVAL 30 DAY)
        WHEN
            DATE(t1.enrollment_date) > '1900-01-01'
                AND DATE(birthdate) > DATE(t1.enrollment_date)
        THEN
            DATE_ADD(birthdate, INTERVAL 30 DAY)
        WHEN
            DATE(t1.enrollment_date) > '1900-01-01'
                AND DATE(birthdate) <= DATE(t1.enrollment_date)
        THEN
            t1.enrollment_date
    END AS RegistrationDate,
    CASE
        WHEN
            DATE(t1.enrollment_date) = '1900-01-01'
                AND DATE(birthdate) <= '1997-01-01'
        THEN
            '1997-01-01'
        WHEN
            DATE(t1.enrollment_date) = '1900-01-01'
                AND DATE(birthdate) > '1997-01-01'
        THEN
            DATE_ADD(birthdate, INTERVAL 30 DAY)
        WHEN
            DATE(t1.enrollment_date) > '1900-01-01'
                AND DATE(birthdate) > DATE(t1.enrollment_date)
        THEN
            DATE_ADD(birthdate, INTERVAL 30 DAY)
        WHEN
            DATE(t1.enrollment_date) > '1900-01-01'
                AND DATE(birthdate) <= DATE(t1.enrollment_date)
        THEN
            t1.enrollment_date
    END AS RegistrationAtCCC,
    NULL AS RegistrationAtPMTCT,
    NULL AS RegistrationAtTBClinic,
    NULL AS PatientSource,
    clinic_county AS Region,
    NULL AS District,
    NULL AS Village,
    NULL AS ContactRelation,
    CASE
        WHEN @last_encounter_date IS NULL THEN @last_encounter_date:=t1.encounter_date
        ELSE @last_encounter_date
    END AS LastVisit,
    NULL AS MaritalStatus,
    NULL AS EducationLevel,
    CASE
        WHEN
            DATE(t1.enrollment_date) = '1900-01-01'
                AND DATE(birthdate) <= '1997-01-01'
        THEN
            '1997-01-01'
        WHEN
            DATE(t1.enrollment_date) = '1900-01-01'
                AND DATE(birthdate) > '1997-01-01'
        THEN
            DATE_ADD(birthdate, INTERVAL 30 DAY)
        WHEN
            DATE(t1.enrollment_date) > '1900-01-01'
                AND DATE(birthdate) > DATE(t1.enrollment_date)
        THEN
            DATE_ADD(birthdate, INTERVAL 30 DAY)
        WHEN
            DATE(t1.enrollment_date) > '1900-01-01'
                AND DATE(birthdate) <= DATE(t1.enrollment_date)
        THEN
            t1.enrollment_date
    END AS DateConfirmedHIVPositive,
    NULL AS PreviousARTExposure,
    NULL AS PreviousARTStartDate,
    'AMRS' AS Emr,
    'Ampath Plus' AS Project,
    mfl.mfl_code as FacilityID,
    CASE
        WHEN @status IS NULL THEN @status:=t1.status
        ELSE @status
    END AS StatusAtCCC,
    NULL AS StatusAtPMTCT,
    NULL AS StatusAtTBClinic,
    NULL AS SatelliteName,
    IF(t1.arv_first_regimen_start_date,
        t1.arv_first_regimen_start_date,
        t1.enrollment_date) AS arv_first_regimen_start_date,
    CASE
        WHEN @rtc_date IS NULL THEN @rtc_date:=t1.rtc_date
        ELSE @rtc_date
    END AS rtc_date,
    IF(t1.arv_first_regimen,
        etl.get_arv_names(t1.arv_first_regimen),
        'unknown') AS arv_first_regimen,
    IF(t1.arv_first_regimen_start_date,
        t1.arv_first_regimen_start_date,
        t1.enrollment_date) AS arv_start_date,
    etl.get_arv_names(t1.cur_arv_meds) AS cur_arv_meds,
    t1.cur_arv_line_strict,
    NULL AS Inschool,
    NULL AS KeyPopulationType,
    NULL AS Orphan,
    NULL AS PatientResidentCounty,
    NULL AS PatientResidentLocation,
    NULL AS PatientResidentSubCounty,
    NULL AS PatientResidentSubLocation,
    NULL AS PatientResidentVillage,
    NULL AS PatientResidentWard,
    NULL AS PatientType,
    'GeneralPopulation' AS PopulationType,
    NULL AS TransferInDate,
    null as DateCreated
    FROM
    etl.hiv_monthly_report_dataset_frozen t1
        INNER JOIN
    ndwr_all_patients_test_build_queue__0 t3 ON (t3.person_id = t1.person_id)
    left join ndwr.mfl_codes mfl on (mfl.location_id = t1.location_id)
WHERE
    enddate = @end_date
ORDER BY t1.encounter_date DESC);

                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_all_patients_test_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_all_patients_test_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_all_patients_test_build_queue__0 t2 using (person_id);'); 
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
                        
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');


END$$
DELIMITER ;
