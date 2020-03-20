DELIMITER $$
CREATE  PROCEDURE `build_NDWR_pharmacy_test`(IN queue_number int, IN queue_size int, IN cycle_size int, IN mFLCode INT)
BEGIN

					set @primary_table := "ndwr_pharmacy_test";
          set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_pharmacy_v1.0";
          set @mFLCode = mFLCode;
          set @query_type="build";

CREATE TABLE IF NOT EXISTS ndwr_pharmacy_test (
  `PatientPK` INT NOT NULL,
  `PatientID` INT NOT NULL,
  `FacilityID` INT NULL,
  `SiteCode` INT NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `VisitID` INT NULL,
  `Drug` VARCHAR(100) NULL,
  `Provider` VARCHAR(50) NULL,
  `DispenseDate` DATETIME NULL,
  `Duration` INT NULL,
  `ExpectedReturn` DATETIME NULL,
  `TreatmentType` VARCHAR(100) NULL,
  `RegimenLine` VARCHAR(200) NULL,
  `PeriodTaken` VARCHAR(100) NULL,
  `ProphylaxisType` VARCHAR(100) NULL
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_pharmacy_test_temp_",queue_number);
                            set @queue_table = concat("ndwr_pharmacy_test_build_queue_",queue_number);                    												

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_pharmacy_test_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_pharmacy_test_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
							drop temporary table if exists ndwr_pharmacy_test_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_pharmacy_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_pharmacy_test_interim;
                          
                         
                          SET @dyn_sql=CONCAT('create temporary table ndwr_pharmacy_test_interim (SELECT  distinct	
                               t1.person_id as PatientPK,
                               t1.person_id as PatientID,
                               @siteCode as FacilityID,
                               @siteCode AS SiteCode,
							                 "AMRS" as Emr,
							                 "Ampath Plus" as Project,
							                 encounter_id as VisitID,
                               etl.get_arv_names(cur_arv_meds) as Drug,
                               "Government" as Provider,
                               encounter_datetime as DispenseDate,
							                 DATEDIFF(rtc_date,encounter_datetime) as Duration,
                               rtc_date as ExpectedReturn,
                               "HIV Treatment" as TreatmentType,
                               null AS RegimenLine,
							                 null as PeriodTaken,
                               null as ProphylaxisType
                                FROM
                                  etl.flat_hiv_summary_v15b t1
                                inner join ndwr_pharmacy_test_build_queue__0 t3 on (t3.person_id = t1.person_id)
                                where 
                                t1.location_id 
                                in (select location_id from ndwr.mfl_codes where mfl_code=',@mFLCode,')
                                and	t1.cur_arv_meds is not null);');
                          
						 SELECT CONCAT('Creating interim table ', @dyn_sql);

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_pharmacy_test_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_pharmacy_test_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_pharmacy_test_build_queue__0 t2 using (person_id);'); 
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
