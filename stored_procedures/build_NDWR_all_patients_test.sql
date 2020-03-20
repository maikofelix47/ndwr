DELIMITER $$
CREATE  PROCEDURE `build_NDWR_all_patients_test`(IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN

					set @primary_table := "ndwr_all_patients_test";
          set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_all_patients_v1.0";
                    SELECT reporting_period FROM ndwr.mfl_period LIMIT 1 INTO @selectedPeriod;
		  SELECT mfl_code FROM ndwr.mfl_period LIMIT 1 INTO @selectedMFLCode;
          SELECT CONCAT('MFL Code ', @selectedMFLCode);
          SELECT CONCAT('MFL Period ', @selectedPeriod);
          set @siteCode:= @selectedMFLCode;
          set @query_type="build";
          set @selectedPeriod := null;
          set @selectedMFLCode := null;

          

CREATE TABLE IF NOT EXISTS ndwr_all_patients_test(
  `patientid` INT NOT NULL,
  `PatientPK` INT NOT NULL,
  `SiteCode` INT NOT NULL,
  `facilityname` VARCHAR(100) NULL,
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
  `project` VARCHAR(50) NULL,
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
  `TransferInDate` DATETIME NULL);

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
                                          
					SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                   SELECT @person_ids_count AS 'num patients to build';

				  end if;

                    set @total_time=0;
                    set @cycle_number = 0;
                    set @last_encounter_date=null;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_all_patients_test_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_all_patients_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop table if exists ndwr_all_patients_test_interim;

                           create  table ndwr_all_patients_test_interim (
                               
                               SELECT 
                                 distinct                                   
                                   t1.person_id as PatientID,
                               t1.person_id as PatientPK,
                               @siteCode as SiteCode,
                               @facilityName as FacilityName,
                               gender AS Gender,
                               birthdate AS DOB,
                               case
                                  when DATE(t1.enrollment_date) = '1900-01-01' AND DATE(birthdate) <= '1997-01-01' THEN '1997-01-01'
                                  when DATE(t1.enrollment_date) = '1900-01-01' AND DATE(birthdate) > '1997-01-01' THEN DATE_ADD(birthdate, INTERVAL 30 DAY)
                                  when DATE(t1.enrollment_date) > '1900-01-01' AND DATE(birthdate) > DATE(t1.enrollment_date) THEN DATE_ADD(birthdate, INTERVAL 30 DAY)
                                  WHEN DATE(t1.enrollment_date) > '1900-01-01' AND DATE(birthdate) <= DATE(t1.enrollment_date)  THEN t1.enrollment_date
                               end as `RegistrationDate`,
                                case
                                  when DATE(t1.enrollment_date) = '1900-01-01' AND DATE(birthdate) <= '1997-01-01' THEN '1997-01-01'
                                  when DATE(t1.enrollment_date) = '1900-01-01' AND DATE(birthdate) > '1997-01-01' THEN DATE_ADD(birthdate, INTERVAL 30 DAY)
                                  when DATE(t1.enrollment_date) > '1900-01-01' AND DATE(birthdate) > DATE(t1.enrollment_date) THEN DATE_ADD(birthdate, INTERVAL 30 DAY)
                                  WHEN DATE(t1.enrollment_date) > '1900-01-01' AND DATE(birthdate) <= DATE(t1.enrollment_date)  THEN t1.enrollment_date
                               end as `RegistrationAtCCC`,
                               null as RegistrationAtPMTCT,
                               null as RegistrationAtTBClinic,
                               null as PatientSource,
                               clinic_county as Region,
                               null as District,
                               null as Village,
                               null as ContactRelation,
  							 case
   								when  @last_encounter_date is null then @last_encounter_date := t1.encounter_date
   								else @last_encounter_date
   							 end as LastVisit, 
                               null as MaritalStatus,
                               null as EducationLevel,
                                 case
                                  when DATE(t1.enrollment_date) = '1900-01-01' AND DATE(birthdate) <= '1997-01-01' THEN '1997-01-01'
                                  when DATE(t1.enrollment_date) = '1900-01-01' AND DATE(birthdate) > '1997-01-01' THEN DATE_ADD(birthdate, INTERVAL 30 DAY)
                                  when DATE(t1.enrollment_date) > '1900-01-01' AND DATE(birthdate) > DATE(t1.enrollment_date) THEN DATE_ADD(birthdate, INTERVAL 30 DAY)
                                  WHEN DATE(t1.enrollment_date) > '1900-01-01' AND DATE(birthdate) <= DATE(t1.enrollment_date)  THEN t1.enrollment_date
                               end as `DateConfirmedHIVPositive`,
                               null as PreviousARTExposure,
                               null as PreviousARTStartDate,
                               'AMRS' as Emr,
                               'Ampath Plus' as Project,
                               @siteCode as FacilityID,
  							 case
   								when  @status is null then @status := t1.status
   								else @status
   							 end as StatusAtCCC, 
                               null as StatusAtPMTCT,
                               null as StatusAtTBClinic,
                               null as SatelliteName,
  							 if(t1.arv_first_regimen_start_date,t1.arv_first_regimen_start_date,t1.enrollment_date) as arv_first_regimen_start_date,
  							 
  							case
   								when  @rtc_date is null then @rtc_date := t1.rtc_date
   								else @rtc_date
   							 end as rtc_date, 							 
  							 if(t1.arv_first_regimen,etl.get_arv_names(t1.arv_first_regimen),'unknown') as arv_first_regimen,
  							 if(t1.arv_first_regimen_start_date,t1.arv_first_regimen_start_date,t1.enrollment_date) as arv_start_date,
  							 etl.get_arv_names(t1.cur_arv_meds) as cur_arv_meds,
  							 t1.cur_arv_line_strict,
                             null as Inschool,
							 null as KeyPopulationType,
							 null as Orphan,
							 null as PatientResidentCounty,
							 null as PatientResidentLocation,
							 null as PatientResidentSubCounty,
							 null as PatientResidentSubLocation,
							 null as PatientResidentVillage,
							 null as PatientResidentWard,
							 null as PatientType,
							 'GeneralPopulation' as PopulationType,
							 null as TransferInDate
							 FROM etl.hiv_monthly_report_dataset_frozen t1 
                             inner join ndwr_all_patients_test_build_queue__0 t3 on (t3.person_id = t1.person_id)             
  						  where enddate=@selectedPeriod
  								and t1.location_id 
  								in (select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  								order by t1.encounter_date desc
                               );
                        
                          
						              SELECT CONCAT('Creating interim table');

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

                         create table if not exists ndwr_all_patients_extract_test (
                               SELECT 
                                 distinct
								                   PatientPK,
								                   patientid AS PatientID,
							                     FacilityID,
                                   SiteCode,
                                   Emr,
                                   project AS Project,
                                   facilityname AS FacilityName,
                                   Gender,
                                   DOB,
                                   RegistrationDate,
                                   RegistrationAtCCC,
                                   RegistrationAtPMTCT,
                                   RegistrationAtTBClinic,
                                   PatientSource,
                                   Region,
                                   District,
                                   Village,
                                   ContactRelation,
  							                   LastVisit, 
                                   MaritalStatus,
                                   EducationLevel,
                                   DateConfirmedHIVPositive,
                                   PreviousARTExposure,
                                   PreviousARTStartDate,
                                   StatusAtCCC, 
                                   StatusAtPMTCT,
                                   StatusAtTBClinic,
                                   SatelliteName,
                                   Inschool,
                                   KeyPopulationType,
                                   Orphan,
                                   PatientResidentCounty,
                                   PatientResidentLocation,
                                   PatientResidentSubCounty,
                                   PatientResidentSubLocation,
                                   PatientResidentVillage,
                                   PatientResidentWard,
                                   PatientType,
                                   PopulationType,
                                   TransferInDate
                               FROM ndwr.ndwr_all_patients_test
                               );

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
