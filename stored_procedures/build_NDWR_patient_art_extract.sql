DELIMITER $$
CREATE  PROCEDURE `build_NDWR_patient_art_extract`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_art_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_art_extract_v1.1";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr_patient_art_extract (
  `PatientPK` INT NOT NULL,
  `SiteCode` INT NULL,
  `PatientID` VARCHAR(30) NULL,
  `FacilityID` INT NOT NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `FacilityName` VARCHAR(50) NULL,
  `DOB` DATETIME NULL,
  `AgeEnrollment` INT NULL,
  `AgeARTStart` INT NULL,
  `AgeLastVisit` INT NULL,
  `RegistrationDate` DATETIME NULL,
  `PatientSource` VARCHAR(50) NULL,
  `Gender` VARCHAR(50) NULL,
  `StartARTDate` DATETIME NULL,
  `PreviousARTStartDate` DATETIME NULL,
  `PreviousARTRegimen` VARCHAR(50) NULL,
  `StartARTAtThisFacility` VARCHAR(50) NULL,
  `StartRegimen` VARCHAR(50) NULL,
  `StartRegimenLine` VARCHAR(100) NULL,
  `StartRegimenLineCategory` VARCHAR(100) NULL,
  `LastARTDate` DATETIME NULL,
  `LastRegimen` VARCHAR(100) NULL,
  `LastRegimenLine` VARCHAR(200) NULL,
  `Duration` INT NULL,
  `ExpectedReturn` DATETIME NULL,
  `Provider` VARCHAR(50) NULL,
  `LastVisit` DATETIME NULL,
  `ExitReason` VARCHAR(100) NULL,
  `ExitDate` DATETIME NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   INDEX patient_art_start_date (PatientID , StartARTDate),
   INDEX patient_art_end_date (PatientID , StartARTDate),
   INDEX patient_id (PatientID),
   INDEX facility_id (FacilityID),
   INDEX site_code (SiteCode),
   INDEX site_code_patient_pk (SiteCode,PatientPK),
   INDEX patient_pk (PatientPK),
   INDEX art_start_date (StartARTDate),
   INDEX date_created (DateCreated)
);
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_art_extract);

                    if(@query_type="build") then

							                    select 'BUILDING..........................................';
                                  set @write_table = concat("ndwr_patient_art_extract_temp_",queue_number);
                                  set @queue_table = concat("ndwr_patient_art_extract_build_queue_",queue_number);                    												

                                  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
                                  PREPARE s1 from @dyn_sql; 
                                  EXECUTE s1; 
                                  DEALLOCATE PREPARE s1;  


                                  SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_art_extract_build_queue limit ', queue_size, ');'); 
                                  PREPARE s1 from @dyn_sql; 
                                  EXECUTE s1; 
                                  DEALLOCATE PREPARE s1;  

                                  SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_art_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
                            set @write_table = concat("ndwr_patient_art_extract_temp_",queue_number);
                            set @queue_table = "ndwr_patient_art_extract_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr_patient_art_extract_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_patient_art_extract_sync_queue
                             (select distinct PatientID from ndwr.ndwr_all_patients where DateCreated >= @last_update);

                      end if;



                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_art_extract_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_art_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_patient_art_extract_interim;
                          
                           SELECT CONCAT('Creating interim table .. ');
                         
                          create temporary table ndwr_patient_art_extract_interim (SELECT  distinct	
                               t1.PatientPK,
							   t1.SiteCode,
                               t1.PatientID,
                               t1.FacilityID,
                               t1.Emr,
							   t1.Project,
                               t1.FacilityName,
                               t1.DOB as DOB,
			                         DATEDIFF(t1.RegistrationDate,DOB)/365.25 as AgeEnrollment,
                               if(sign(DATEDIFF(t1.arv_first_regimen_start_date,DOB)/365.25)=-1,DATEDIFF(t1.RegistrationDate,DOB)/365.25, DATEDIFF(t1.arv_first_regimen_start_date,t1.DOB)/365.25) as AgeARTStart,
			                         DATEDIFF(t1.lastVisit,t1.DOB)/365.25 as AgeLastVisit,
                               RegistrationDate,
                               null as PatientSource,
                               t1.gender as Gender,
                               case
                                    when RegistrationDate <= if(DATE(t1.arv_first_regimen_start_date) = "1900-01-01","1997-01-01",t1.arv_first_regimen_start_date) then RegistrationDate
                                    when RegistrationDate > if(DATE(t1.arv_first_regimen_start_date) = "1900-01-01","1997-01-01",t1.arv_first_regimen_start_date) then DATE_ADD(RegistrationDate, INTERVAL 30 DAY)
                               end as StartARTDate,
                              case
                                when RegistrationDate <= if(DATE(t1.arv_first_regimen_start_date) = "1900-01-01","1997-01-01",t1.arv_first_regimen_start_date) then RegistrationDate
                                when RegistrationDate > if(DATE(t1.arv_first_regimen_start_date) = "1900-01-01","1997-01-01",t1.arv_first_regimen_start_date) then DATE_ADD(RegistrationDate, INTERVAL 30 DAY)
                              end as PreviousARTStartDate,
                              t1.arv_first_regimen as PreviousARTRegimen,
                              t1.arv_start_date as StartARTAtThisFacility,
			                        t1.arv_first_regimen as StartRegimen,
							 t1.cur_arv_line AS StartRegimenLine,
                              CASE
                                WHEN t1.cur_arv_line = 1 AND  (DATEDIFF(t1.lastVisit, t1.DOB) / 365.25) >= 25 THEN "Adult FirstLine"
                                WHEN t1.cur_arv_line = 1 AND  (DATEDIFF(t1.lastVisit, t1.DOB) / 365.25) < 25 THEN "Child FirstLine"
                                WHEN t1.cur_arv_line = 2 AND  (DATEDIFF(t1.lastVisit, t1.DOB) / 365.25) >= 25 THEN "Adult SecondLine"
                                WHEN t1.cur_arv_line = 2 AND  (DATEDIFF(t1.lastVisit, t1.DOB) / 365.25) < 25 THEN "Child SecondLine"
                                WHEN t1.cur_arv_line = 3 AND  (DATEDIFF(t1.lastVisit, t1.DOB) / 365.25) >= 25 THEN "Adult ThirdLine"
                                WHEN t1.cur_arv_line = 3 AND  (DATEDIFF(t1.lastVisit, t1.DOB) / 365.25) < 25 THEN "Child ThirdLine"
                                ELSE NULL
                              END AS StartRegimenLineCategory,
                              t1.lastVisit as LastARTDate,
							  t1.cur_arv_meds as LastRegimen,
							  t1.cur_arv_line as LastRegimenLine,
                              DATEDIFF(t1.rtc_date,t1.lastVisit) as Duration,
			                        t1.rtc_date as ExpectedReturn,
                              "Government" as Provider,
                              t1.LastVisit,
			                        if(t1.StatusAtCCC in("dead","ltfu","transfer_out"),StatusAtCCC,null) as ExitReason,
			                        if(t1.StatusAtCCC in("dead","ltfu","transfer_out"),t1.lastVisit,null) as ExitDate,
							  null as DateCreated
                              FROM ndwr.ndwr_all_patients_extract t1
                              join ndwr_patient_art_extract_build_queue__0 b on (b.person_id = t1.PatientPK)
                              
                              );
                          
						

                         

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_art_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_art_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_art_extract_build_queue__0 t2 using (person_id);'); 
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
