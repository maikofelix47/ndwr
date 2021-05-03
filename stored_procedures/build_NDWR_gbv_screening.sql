DELIMITER $$
CREATE  PROCEDURE `build_ndwr_gbv_screening`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_gbv_screening";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_gbv_screening_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr_gbv_screening (
  `PatientPK` INT NOT NULL,
  `SiteCode` INT NOT NULL,
  `PatientID` VARCHAR(30) NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `FacilityName` VARCHAR(100) NULL,
  `PartnerPersonID`  INT NULL,
  `VisitID` INT NULL,
  `VisitDate` DATETIME NOT NULL,
  `IPV` BOOLEAN NULL,
  `PhysicalIPV` VARCHAR(10) NULL,
  `EmotionalIPV` VARCHAR(10) NULL,
  `SexualIPV` VARCHAR(10) NULL,
  `IPVRelationship` VARCHAR(10) NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   INDEX patient_gbv_pk (PatientPK),
   INDEX patient_gbv_sc (SiteCode),
   INDEX patient_gbv_visit_id (VisitID),
   INDEX patient_pgbv_k_site_list (PatientPK,SiteCode),
   INDEX date_created (DateCreated)
);
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_gbv_screening);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_gbv_screening_temp_",queue_number);
                            set @queue_table = concat("ndwr_gbv_screening_build_queue_",queue_number);                    												

										        SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_gbv_screening_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_gbv_screening_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  
                                          
					                  SET @person_ids_count = 0;
                            SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;

                            SELECT @person_ids_count AS 'num patients to build';
                   
                            SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ', @queue_table ,' t2 on (t2.person_id = t1.PatientID);'); 
				                    SELECT CONCAT('Deleting patient records in interim ', @primary_table);
				                    PREPARE s1 from @dyn_sql; 
				                    EXECUTE s1; 
				                    DEALLOCATE PREPARE s1;  

				              end if;
                      if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_gbv_screening_temp_",queue_number);
                            set @queue_table = "ndwr_gbv_screening_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr_gbv_screening_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_gbv_screening_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b where date_created >= @last_update);

                      end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_gbv_screening_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_gbv_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                          drop temporary table if exists ndwr_gbv_screening_interim;
                          create temporary table ndwr_gbv_screening_interim(
                              SELECT
                              c.PatientPK,
                              c.SiteCode,
                              c.PatientID,
                              c.Emr,
                              c.Project,
                              c.FacilityName,
                              c.PartnerPersonID,
                              c.VisitID,
                              c.VisitDate,
                              CASE
                                WHEN c.IpvScreeningOutcome IN (9303,1789) THEN 1
                                WHEN c.IpvScreeningOutcome IN (1107) then 0
                                ELSE NULL
                              END AS 'IPV',
                              CASE
                                WHEN c.IpvScreeningOutcome IN (1789) AND c.RelationshipWithPatient in (1670,1669,5716,10479) THEN 1
                                WHEN c.IpvScreeningOutcome IN (1107) THEN 0
                                ELSE NULL
                              END AS 'PhysicalIPV',
                              CASE
                                WHEN c.IpvScreeningOutcome IN (7020) AND c.RelationshipWithPatient in (1670,1669,5716,10479) THEN 1
                                WHEN c.IpvScreeningOutcome IN (1107) THEN 0
                                ELSE NULL
                              END AS 'EmotionalIPV',
                              CASE
                                WHEN c.IpvScreeningOutcome IN (9303) AND c.RelationshipWithPatient in (1670,1669,5716,10479) THEN 1
                                WHEN c.IpvScreeningOutcome IN (1107) THEN 0
                                ELSE NULL
                              END AS 'SexualIPV',
                              CASE
                                 WHEN c.IpvScreeningOutcome IN (1789,7020,9303) AND c.RelationshipWithPatient NOT IN (1670,1669,5716,10479) THEN 1
                                 WHEN c.IpvScreeningOutcome IN (1107) THEN 0
                                 ELSE NULL
                              END AS 'IPVRelationship',
                              NULL AS 'DateCreated'
                              FROM
                              ndwr.ndwr_patient_contact_listing c


                          );
                          
                         
                          
                          
						 SELECT CONCAT('Creating interim table');

                         

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_gbv_screening_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_gbv_screening_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_gbv_screening_build_queue__0 t2 using (person_id);'); 
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
