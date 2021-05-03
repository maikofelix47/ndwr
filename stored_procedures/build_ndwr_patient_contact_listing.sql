DELIMITER $$
CREATE  PROCEDURE `build_ndwr_patient_contact_listing`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_contact_listing";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_contact_listing_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr_patient_contact_listing (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientID` VARCHAR(30) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `VisitID` INT NOT NULL,
    `VisitDate` DATETIME NOT NULL,
    `PartnerPersonID` VARCHAR(100) NULL,
    `ContactAge` INT NULL,
    `ContactSex` TINYINT NULL,
    `ContactMaritalStatus` INT NULL,
    `RelationshipWithPatient` INT NULL,
    `ScreenedForIpv` BOOLEAN NULL,
    `IpvScreening` INT NULL,
    `IpvScreeningOutcome` INT NULL,
    `CurrentlyLivingWithIndexClient` BOOLEAN NULL,
    `KnowledgeOfHivStatus` BOOLEAN NULL,
    `PnsApproach` INT NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX patient_cl_pk (PatientPK),
    INDEX patient_cl_pid (PatientID),
    INDEX patient_cl_sc (SiteCode),
    INDEX patient_pk_site_list (PatientPK , SiteCode),
    INDEX date_created (DateCreated)
);
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_contact_listing);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_patient_contact_listing_temp_",queue_number);
                            set @queue_table = concat("ndwr_patient_contact_listing_build_queue_",queue_number);                    												

										        SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_contact_listing_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_contact_listing_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
				SELECT 
    CONCAT('Deleting patient records in interim ',
            @primary_table);
				                    PREPARE s1 from @dyn_sql; 
				                    EXECUTE s1; 
				                    DEALLOCATE PREPARE s1;  

				              end if;
                      if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_patient_contact_listing_temp_",queue_number);
                            set @queue_table = "ndwr_patient_contact_listing_sync_queue";
CREATE TABLE IF NOT EXISTS ndwr_patient_contact_listing_sync_queue (
    person_id INT PRIMARY KEY
);                            
                            
                            set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    ndwr.flat_log
WHERE
    table_name = @table_version;

                            replace into ndwr_patient_contact_listing_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b where date_created >= @last_update);

                      end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_contact_listing_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_contact_listing_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;


                        -- create a temporary table to seperate contact listing obs_group to distinct obs listing

                            drop temporary table if exists contact_listing_flat_obs;
                            
SELECT CONCAT('Creating contact listig flat obs');

                            set @boundary := "!!";
                            create temporary table contact_listing_flat_obs(select
                                o.obs_group_id,
                                o.person_id,
                                e.visit_id,
                                o.encounter_id,
                                e.encounter_datetime,
                                e.encounter_type,
                                e.location_id,
                                group_concat(
                                    case
                                        when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
                                        when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
                                        when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
                                        when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
                                        when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
                                        when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
                                    end
                                    order by o.concept_id,value_coded
                                    separator ' ## '
                                ) as obs,

                                group_concat(
                                    case
                                        when value_coded is not null or value_numeric is not null or value_datetime is not null or  value_text is not null or value_drug is not null or value_modifier is not null
                                        then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
                                    end
                                    order by o.concept_id,value_coded
                                    separator ' ## '
                                ) as obs_datetimes,
                                max(o.date_created) as max_date_created
                                    from ndwr_patient_contact_listing_build_queue__0 q 
                                    join amrs.obs o on (q.person_id = o.person_id)
                                    join amrs.encounter e on (e.encounter_id = o.encounter_id)
                                where
                                    e.encounter_type in (243) AND o.obs_group_id IS NOT NULL
                                group by o.obs_group_id
                            );
                                      
						  
                          
                          
                          
                          drop temporary table if exists ndwr_patient_contact_listing_interim;

                          
                         
                          create temporary table ndwr_patient_contact_listing_interim (
                                        SELECT 
                                        c.person_id AS 'PatientPK',
                                        mfl.mfl_code AS 'SiteCode',
                                        i.identifier AS 'PatientID',
                                        'AMRS' AS 'Emr',
                                        'AMPATH' AS 'Project',
                                        mfl.Facility AS 'FacilityName',
                                        c.encounter_id as 'VisitID',
                                        c.encounter_datetime as 'VisitDate',
                                        CASE
                                            WHEN c.obs REGEXP '!!9775=' THEN etl.GetValues(c.obs,9775)
                                            ELSE NULL
                                        END AS 'PartnerPersonID',
                                        CASE
                                            WHEN c.obs REGEXP '!!11729=' THEN etl.GetValues(c.obs,11729)
                                            ELSE NULL
                                        END AS 'ContactAge',
                                        CASE
                                            WHEN c.obs REGEXP '!!10981=6226' THEN 1
                                            WHEN c.obs REGEXP '!!10981=6227' THEN 2
                                            ELSE NULL
                                        END AS 'ContactSex',
                                        CASE
                                            WHEN c.obs REGEXP '!!1054=' THEN etl.GetValues(c.obs,1054)
                                            ELSE NULL
                                        END AS 'ContactMaritalStatus',
                                        CASE
                                            WHEN c.obs REGEXP '!!1675=' THEN etl.GetValues(c.obs,1675)
                                            ELSE NULL
                                        END AS 'RelationshipWithPatient',
                                        CASE
                                            WHEN c.obs REGEXP '!!11740=1065' THEN 1
                                            WHEN c.obs REGEXP '!!11740=1066' THEN 0
                                            ELSE NULL
                                        END AS 'ScreenedForIpv',
                                        CASE
                                            WHEN c.obs REGEXP '!!11739=' THEN etl.GetValues(c.obs,11739)
                                            ELSE NULL
                                        END AS 'IpvScreening',
                                        CASE
                                            WHEN c.obs REGEXP '!!11739=' THEN etl.GetValues(c.obs,11739)
                                            ELSE NULL
                                        END AS 'IpvScreeningOutcome',
                                        CASE
                                            WHEN c.obs REGEXP '!!11674=1065' THEN 1
                                            WHEN c.obs REGEXP '!!11674=1066' THEN 0
                                            ELSE NULL
                                        END AS 'CurrentlyLivingWithIndexClient',
                                        CASE
                                            WHEN c.obs REGEXP '!!7001=1065' THEN 1
                                            WHEN c.obs REGEXP '!!7001=1066' THEN 0
                                            ELSE NULL
                                        END AS 'KnowledgeOfHivStatus',
                                        CASE
                                            WHEN c.obs REGEXP '!!11735=' THEN etl.GetValues(c.obs,11735)
                                            ELSE NULL
                                        END AS 'PnsApproach',
                                        null as 'DateCreated'
                                    FROM
                                        contact_listing_flat_obs c
                                        JOIN
                                        ndwr.mfl_codes mfl ON (mfl.location_id = c.location_id)
                                        left join amrs.patient_identifier i on (i.patient_id = c.person_id AND i.identifier_type = 28 AND i.voided = 0)
                          );
                          
						SELECT CONCAT('Creating interim table');

                         

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_contact_listing_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_contact_listing_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_contact_listing_build_queue__0 t2 using (person_id);'); 
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
