DELIMITER $$
CREATE  PROCEDURE `build_NDWR_patient_allergies_chronic_illness`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_allergies_chronic_illness";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_allergies_chronic_illness_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr.ndwr_patient_allergies_chronic_illness (
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientID` VARCHAR(30) NULL,
    `FacilityID` INT NOT NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(50) NULL,
    `VisitID` INT NULL,
    `VisitDate` DATETIME NULL,
    `ChronicIllness` VARCHAR(200) NULL,
    `ChronicOnsetDate` DATETIME NULL,
    `knownAllergies` BOOLEAN NULL,
    `AllergyCausativeAgent` VARCHAR(30) NULL,
    `AllergicReaction` VARCHAR(200) NULL,
    `AllergySeverity` VARCHAR(200) NULL,
    `AllergyOnsetDate` DATETIME NULL,
    `Skin` VARCHAR(200) NULL,
    `Eyes` VARCHAR(200) NULL,
    `ENT` VARCHAR(200) NULL,
    `Chest` VARCHAR(200) NULL,
    `CVS` VARCHAR(200) NULL,
    `Abdomen` VARCHAR(200) NULL,
    `CNS` VARCHAR(200) NULL,
    `Genitourinary` VARCHAR(200) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY VisitID (VisitID),
    INDEX patient_id (PatientID),
    INDEX patient_pk (PatientPK),
    INDEX aci_site_code (SiteCode),
    INDEX aci_site_code_visit (SiteCode , VisitID),
    INDEX aci_site_code_pk (SiteCode , PatientPK),
    INDEX date_created (DateCreated)
);
                    set @last_date_created := null;

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_patient_allergies_chronic_illness_temp_",queue_number);
                            set @queue_table = concat("ndwr_patient_allergies_chronic_illness_build_queue_",queue_number); 
                            
                                          
                               

										   SET @dyn_sql=CONCAT('create table if not exists ndwr.',@write_table,' like ndwr.',@primary_table);
                                           PREPARE s1 from @dyn_sql; 
                                           EXECUTE s1; 
                                           DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_allergies_chronic_illness_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_allergies_chronic_illness_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_allergies_chronic_illness_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_allergies_chronic_illness_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
                                      
						 
                          

SELECT CONCAT('Creating ndwr_patient_allergies...');
                          
						  drop temporary table if exists ndwr_patient_allergies;
                          CREATE temporary TABLE ndwr_patient_allergies(
                              SELECT 
                                    o.person_id,
                                    o.encounter_id,
                                    o.encounter_datetime,
                                    o.encounter_type,
                                    o.location_id,
                                    CASE
                                        WHEN (o.obs REGEXP '!!6011=1065' || o.obs REGEXP '!!6012=1065') THEN 1
                                        ELSE NULL
                                    END AS 'knownAllergies',
                                    CASE
                                        WHEN o.obs REGEXP '!!6011=1065'  AND o.obs REGEXP '!!6012=1065' THEN 'Penicillin | Sulfa'
                                        WHEN o.obs REGEXP '!!6011=1065'  THEN 'Penicillin'
                                        WHEN o.obs REGEXP '!!6012=1065'  THEN 'Sulfa'
                                        ELSE NULL
                                    END AS 'AllergyCausativeAgent',
                                    CASE
                                        WHEN o.obs REGEXP '!!1123=639!!' THEN 'RIGHT LOWER LOBE'
                                        WHEN o.obs REGEXP '!!1123=1115!!'   THEN 'NORMAL'
                                        WHEN o.obs REGEXP '!!1123=1116!!'  THEN 'ABNORMAL'
                                        WHEN o.obs REGEXP '!!1123=5115!!'  THEN 'DIMINISHED BREATH SOUNDS'
                                        WHEN o.obs REGEXP '!!1123=5116!!'   THEN 'BRONCHIAL BREATH SOUNDS'
                                        WHEN o.obs REGEXP '!!1123=5127!!'  THEN 'CREPITATIONS'
                                        WHEN o.obs REGEXP '!!1123=5134!!'   THEN 'LEFT LOWER LOBE'
                                        WHEN o.obs REGEXP '!!1123=5138!!'   THEN 'DULLNESS TO PERCUSSION'
                                        WHEN o.obs REGEXP '!!1123=5139!!'  THEN 'LEFT'
                                        WHEN o.obs REGEXP '!!1123=5141!!'   THEN 'RIGHT'
                                        WHEN o.obs REGEXP '!!1123=5181!!'   THEN 'RHONCHI'
                                        WHEN o.obs REGEXP '!!1123=5209!!'   THEN 'WHEEZE'
                                        WHEN o.obs REGEXP '!!1123=5622!!'   THEN 'OTHER'
                                        WHEN o.obs REGEXP '!!1123=1107!!'  THEN 'NONE'
                                        WHEN o.obs REGEXP '!!1123=2398!!'   THEN 'PERIHILAR'
                                        WHEN o.obs REGEXP '!!1123=2399!!'  THEN 'BILATERAL'
                                        WHEN o.obs REGEXP '!!1123=576!!'   THEN 'DIFFUSE (576)'
                                        WHEN o.obs REGEXP '!!1123=5119!!'   THEN 'RIGHT UPPER LOBE (5119)'
                                        WHEN o.obs REGEXP '!!1123=5132!!'   THEN 'LEFT UPPER LOBE'
                                        ELSE NULL
                                    END AS 'Chest',
                                    CASE
										WHEN o.obs REGEXP "!!1126=1115!!" then 'NORMAL'
										WHEN o.obs REGEXP "!!1126=1116!!" then 'ABNORMAL'
										WHEN o.obs REGEXP "!!1126=1118!!" then 'NOT DONE'
										WHEN o.obs REGEXP "!!1126=2186!!" then 'SIGN OF SEXUAL ABUSE'
										WHEN o.obs REGEXP "!!1126=864!!" then 'GENITAL SORES'
										WHEN o.obs REGEXP "!!1126=6334!!" then 'FEMALE GENITAL MUTILATION'
										WHEN o.obs REGEXP "!!1126=1447!!" then 'GENITAL WARTS'
										WHEN o.obs REGEXP "!!1126=5993!!" then 'VAGINAL DISCHARGE'
										WHEN o.obs REGEXP "!!1126=8998!!" then 'RUPTURE OF MEMBRANES'
										WHEN o.obs REGEXP "!!1126=1489!!" then 'VAGINAL BLEEDING'
										WHEN o.obs REGEXP "!!1126=8417!!" then 'POLYURIA'
										WHEN o.obs REGEXP "!!1126=6261!!" then 'SIGNS OF LABOR'
                                    ELSE NULL
                                    END AS 'Genitourinary'
                                FROM
                                    ndwr.ndwr_patient_allergies_chronic_illness_build_queue__0 q
                                        JOIN
                                    etl.flat_obs o ON (q.person_id = o.person_id)
                                WHERE
                                    o.encounter_type IN (1,106)
                          );

                          

SELECT CONCAT('Creating ndwr_patient_allergies_physical_findings...');
                          
                          drop temporary table if exists ndwr_patient_allergies_physical_findings;

                          CREATE temporary TABLE ndwr_patient_allergies_physical_findings(
                              SELECT
                                    b.person_id,
                                    b.encounter_id,
                                    b.location_id,
                                    GROUP_CONCAT(DISTINCT b.Skin SEPARATOR ' | ') AS `Skin`,
                                    GROUP_CONCAT(DISTINCT b.ENT SEPARATOR ' | ') AS `ENT`,
                                    GROUP_CONCAT(DISTINCT b.CVS SEPARATOR ' | ') AS `CVS`,
                                    GROUP_CONCAT(DISTINCT b.Abdomen SEPARATOR ' | ') AS `Abdomen`,
                                    GROUP_CONCAT(DISTINCT b.CNS SEPARATOR ' | ') AS `CNS`,
                                    GROUP_CONCAT(DISTINCT b.AllergicReaction SEPARATOR ' | ') AS `AllergicReaction`
                                    from
                                    (SELECT 
                                        o.person_id,
                                        o.encounter_id,
                                        o.location_id,
                                        o.concept_id,
                                        o.value_coded,
                                        o.obs_group_id,
                                        o.obs_id,
                                        CASE
                                            WHEN o.concept_id = 1120 THEN cn.name
                                        END AS 'Skin',
                                         CASE
                                            WHEN o.concept_id = 2085 THEN cn.name
                                            ELSE NULL
                                        END AS 'AllergicReaction',
                                        CASE
                                            WHEN o.concept_id = 1122 THEN cn.name
                                        END AS 'ENT',
                                        CASE
                                            WHEN o.concept_id = 1124 THEN cn.name
                                        END AS 'CVS',
                                        CASE
                                            WHEN o.concept_id = 1125 THEN cn.name
                                        END AS 'Abdomen',
                                        CASE
                                            WHEN o.concept_id = 1129 THEN cn.name
                                        END AS 'CNS'
                                    FROM
                                        ndwr_patient_allergies q
                                            JOIN
                                        amrs.obs o ON (q.person_id = o.person_id AND q.encounter_id = o.encounter_id and o.voided = 0)
                                            JOIN
                                        amrs.concept_name cn ON (cn.concept_id = o.value_coded
                                            AND cn.voided = 0
                                            AND cn.locale_preferred = 1)
                                    WHERE
                                        o.concept_id IN (1120,2085,1122,1124,1125,1129)
                                        group by o.person_id,o.encounter_id,o.obs_id
                                        ) b
                                        group by b.encounter_id
                          );

SELECT CONCAT('Creating chronic illness table ....');
                         drop temporary table if exists chronic_illness;
                         create  temporary table chronic_illness(
                             SELECT 
                                    o.person_id,
                                    o.encounter_id,
                                    o.obs_datetime,
                                    o.location_id,
                                    o.concept_id,
                                    o.value_coded,
                                    o.obs_group_id,
                                    o.obs_id,
                                    GROUP_CONCAT(DISTINCT cn.name SEPARATOR ' | ') AS `ChronicIllness`
                                FROM
                                    ndwr_patient_allergies q
                                        JOIN
                                    amrs.obs o ON (q.person_id = o.person_id and o.voided = 0)
                                        JOIN
                                    amrs.concept_name cn ON (cn.concept_id = o.value_coded
                                        AND cn.voided = 0
                                        AND cn.locale_preferred = 1)
                                WHERE
                                    o.concept_id IN (6042)
                                    AND o.obs_group_id IS not NULL
                                GROUP BY o.person_id, o.encounter_id
                         );

                          
						SELECT CONCAT('Creating interim table');

                         drop temporary table if exists ndwr_patient_allergies_chronic_illness_interim;
                         create temporary table ndwr_patient_allergies_chronic_illness_interim(
                             SELECT
                              a.person_id AS 'PatientPK',
                              mfl.mfl_code AS 'SiteCode',
                              t.PatientID as 'PatientID',
                               mfl.mfl_code as 'FacilityID',
                               t.Emr as 'Emr',
							   t.Project as 'Project',
                               mfl.Facility AS 'FacilityName',
                               a.encounter_id as 'VisitID',
                               a.encounter_datetime as 'VisitDate',
                               c.ChronicIllness,
                               NULL AS 'ChronicOnsetDate',
                               a.knownAllergies,
                               a.AllergyCausativeAgent,
                               f.AllergicReaction,
                               NULL AS 'AllergySeverity',
                               NULL AS 'AllergyOnsetDate',
                               f.Skin,
                               NULL AS 'Eyes',
                               f.ENT,
                               a.Chest,
                               f.CVS,
                               f.Abdomen,
                               f.CNS,
                               a.Genitourinary,
                               NULL AS 'DateCreated'
                             from
                             ndwr_patient_allergies a
                             left join ndwr_patient_allergies_physical_findings f on (f.person_id = a.person_id AND f.encounter_id = a.encounter_id)
                             left join chronic_illness c on (c.person_id = a.person_id AND c.encounter_id = a.encounter_id)
                             join ndwr.mfl_codes mfl ON (mfl.location_id = a.location_id)
                             join ndwr.ndwr_all_patients_extract t on (t.PatientPK = a.person_id)
                         );

                         -- Remove encounters with no chronic or allergy data
                         SELECT CONCAT('Remove encounters with no chronic or allergy data ..');
                         DELETE FROM ndwr_patient_allergies_chronic_illness_interim 
                            WHERE
                                ChronicIllness IS NULL 
                                AND knownAllergies IS NULL
                                AND AllergyCausativeAgent IS NULL
                                AND AllergicReaction IS NULL
                                AND Skin IS NULL
                                AND Eyes IS NULL
                                AND ENT IS NULL
                                AND Chest IS NULL
                                AND CVS IS NULL
                                AND Abdomen IS NULL
                                AND CNS IS NULL
                                AND Genitourinary IS NULL;

                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_allergies_chronic_illness_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr.ndwr_patient_allergies_chronic_illness_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_allergies_chronic_illness_build_queue__0 t2 using (person_id);'); 
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
