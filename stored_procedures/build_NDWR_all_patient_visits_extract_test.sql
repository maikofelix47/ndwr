DELIMITER $$
CREATE  PROCEDURE `build_NDWR_all_patient_visits_extract_test`(IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN

					set @primary_table := "ndwr_all_patient_visits_extract_test";
          set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_all_patient_visits_extract_v1.0";
          set @query_type="build";

          SELECT reporting_period FROM ndwr.mfl_period LIMIT 1 INTO @selectedPeriod;
		  SELECT mfl_code FROM ndwr.mfl_period LIMIT 1 INTO @selectedMFLCode;

           set @siteCode:= @selectedMFLCode; 

CREATE TABLE IF NOT EXISTS `ndwr`.`ndwr_all_patient_visits_extract_test` (
  `PatientPK` INT NOT NULL,
  `PatientID` INT NOT NULL,
  `FacilityID` INT NULL,
  `SiteCode` VARCHAR(45) NOT NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `FacilityName` VARCHAR(100) NULL,
  `VisitID` INT NULL,
  `VisitDate` DATETIME NULL,
  `Service` VARCHAR(50) NULL,
  `VisitType` VARCHAR(50) NULL,
  `WHOStage` INT NULL,
  `WABStage` INT NULL,
  `Pregnant` VARCHAR(20) NULL,
  `LMP` DATETIME NULL,
  `EDD` DATETIME NULL,
  `Height` INT NULL,
  `Weight` INT NULL,
  `BP` VARCHAR(10) NULL,
  `OI` VARCHAR(200) NULL,
  `OIDate` DATETIME NULL,
  `Adherence` VARCHAR(200) NULL,
  `AdherenceCategory` VARCHAR(200) NULL,
  `SubstitutionFirstlineRegimenDate` DATETIME NULL,
  `SubstitutionFirstlineRegimenReason` VARCHAR(500) NULL,
  `SubstitutionSecondlineRegimenDate` DATETIME NULL,
  `SubstitutionSecondlineRegimenReason` VARCHAR(500) NULL,
  `SecondlineRegimenChangeDate` DATETIME NULL,
  `SecondlineRegimenChangeReason` VARCHAR(500) NULL,
  `FamilyPlanningMethod` VARCHAR(200) NULL,
  `PwP` VARCHAR(200) NULL,
  `GestationAge` INT NULL,
  `NextAppointmentDate` DATETIME NULL,
  `DifferentiatedCare` VARCHAR(50) NULL,
  `KeyPopulationType` VARCHAR(50) NULL,
  `PopulationType` VARCHAR(50) NULL,
  `StabilityAssessment` VARCHAR(50) NULL);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_all_patient_visits_extract_test_temp_",queue_number);
                            set @queue_table = concat("ndwr_all_patient_visits_extract_test_build_queue_",queue_number);                    												

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_all_patient_visits_extract_test_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_all_patient_visits_extract_test_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_all_patient_visits_extract_test_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_all_patient_visits_extract_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						SELECT CONCAT('Deleting data from ', @primary_table);
                                      
						 SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ndwr_all_patient_visits_extract_test_build_queue__0 t2 on (t1.PatientPK = t2.person_id);'); 
						 PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
					                DEALLOCATE PREPARE s1; 
                                      
						  
                drop temporary table if exists ndwr_all_patient_visits_extract_test_interim;

                SELECT CONCAT('Creating and populating interim table ..');
                          
                create temporary table ndwr_all_patient_visits_extract_test_interim (
                       SELECT distinct
						   e.person_id AS PatientPK,
                           e.person_id AS PatientID,
   						   @siteCode  as FacilityID,
                           @siteCode AS SiteCode,
						   'AMRS' AS Emr,
						   'Ampath Plus' AS Project,
                            @facilityName AS FacilityName,
                           e.encounter_id AS VisitID,
                           e.encounter_datetime AS VisitDate,
                           'HIV Care' as Service,
                           if(cn.name is not null,cn.name,'Unknownknown') as VisitType,
                           e.cur_who_stage AS WHOStage,
                           null  AS WABStage,
                           e.pregnant AS Pregnant,
                           e.LMP AS LMP,
                           e.edd as EDD,
                           v.Height AS Height,
						   v.Weight AS Weight,
                           v.bp AS BP,
                           o.OI AS OI,
                           o.OIDate AS OIDate,
                           e.cur_arv_adherence AS Adherence,
                           e.cur_arv_adherence AS AdherenceCategory,
                           null as SubstitutionFirstlineRegimenDate,
                           null as SubstitutionFirstlineRegimenReason,
                           null as SubstitutionSecondlineRegimenDate,
                           null as SubstitutionSecondlineRegimenReason,
						   null as SecondlineRegimenChangeDate,
						   null as SecondlineRegimenChangeReason,
                           e.family_planning AS FamilyPlanningMethod,
                           e.pwp AS PwP,
                           e.gestation AS GestationAge,
						   case
                             when e.rtc_date IS NOT NULL then e.rtc_date
                             ELSE DATE_ADD(e.encounter_datetime, INTERVAL 21 DAY)
                           end as NextAppointmentDate,
                           null as DifferentiatedCare,
                           null as KeyPopulationType,
                           'General Population' as PopulationType,
                           null as StabilityAssessment
					
                           FROM  ndwr.ndwr_visit_0 e 
                           inner join ndwr_all_patient_visits_extract_test_build_queue__0 t3 on (t3.person_id = e.person_id)
                           left join ndwr_vitals v on v.person_id=e.person_id and v.encounter_datetime=e.encounter_datetime
                           left join ndwroi  o  on o.person_id=e.person_id and o.OIDate=e.encounter_datetime
						                left join amrs.concept_name cn on cn.concept_id=e.scheduled_visit and cn.concept_name_type='FULLY_SPECIFIED' and voided<>1
                       ); 
                          
						 SELECT CONCAT('Created interim table ..');


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_all_patient_visits_extract_test_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_all_patient_visits_extract_test_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_all_patient_visits_extract_test_build_queue__0 t2 using (person_id);'); 
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
