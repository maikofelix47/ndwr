DELIMITER $$
CREATE  PROCEDURE `build_NDWR_all_patients_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_all_patients_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_all_patients_v1.1";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_all_patients_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_all_patients_extract (
    `Pkv` VARCHAR(20) NULL,
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientID` VARCHAR(30) NULL,
    `FacilityID` INT NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
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
    `StatusAtCCC` VARCHAR(100) NULL,
    `StatusAtPMTCT` VARCHAR(100) NULL,
    `StatusAtTBClinic` VARCHAR(100) NULL,
    `Inschool` VARCHAR(10) NULL,
    `arv_first_regimen_start_date` DATE NULL,
    `rtc_date` DATE NULL,
    `arv_first_regimen` VARCHAR(200) NULL,
    `arv_start_date` DATE NULL,
    `cur_arv_meds` VARCHAR(200) NULL,
    `cur_arv_line_strict` INT NULL,
    `cur_arv_line` INT NULL,
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
    `Occupation` VARCHAR(100) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     INDEX patient_patient_id (PatientID),
     INDEX patient_patient_pk (PatientPK),
     INDEX patient_facility_id (FacilityID),
     INDEX patient_site_code (SiteCode),
     INDEX patient_date_created (DateCreated),
     INDEX patient_patient_facility (PatientID,FacilityID)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_all_patients_temp_",queue_number);
                            set @queue_table = concat("ndwr_all_patients_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_all_patients_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_all_patients_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = concat("ndwr_all_patients_temp_",queue_number);
                            
                            SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;
                            
                            set @queue_table = "ndwr_all_patients_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr.ndwr_all_patients_sync_queue (
                                person_id INT(6) UNSIGNED,
                                INDEX all_patients_sync_person_id (person_id)
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_all_patients_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b WHERE
                   is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL and date_created >= @last_update);
                   
                   replace into ndwr.ndwr_all_patients_sync_queue(
					SELECT 
					DISTINCT PatientID
					FROM
						ndwr.ndwr_all_patients_extract
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
							drop  table if exists ndwr_all_patients_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_all_patients_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;

                         select CONCAT('Creating soundex mapping ...');

                          drop temporary table if exists ndwr_patient_pkv_occcupation_mapping;


                          create temporary TABLE  ndwr_patient_pkv_occcupation_mapping(
                              SELECT 
                                    q.person_id,
                                    p.gender,
                                    p.birthdate,
									i.identifier as 'PatientID',
                                    cn.name as 'Occupation',
                                    CONCAT(p.gender,n.given_name,SOUNDEX(n.given_name),SOUNDEX(n.family_name),DATE_FORMAT(p.birthdate,'%Y')) as 'Pkv'
                                FROM
                                    ndwr.ndwr_all_patients_build_queue__0 q
                                    left join amrs.person p on (p.person_id = q.person_id AND p.voided = 0)
                                    left join amrs.person_name n on (n.person_id = q.person_id AND n.voided = 0)
                                    left join amrs.person_attribute a on (a.person_id = q.person_id AND a.person_attribute_type_id = 42 AND a.voided = 0)
                                    left join amrs.concept_name cn on (cn.concept_id = a.value and cn.locale_preferred = 1 AND a.value != 5622)
                                    left join amrs.patient_identifier i on (i.patient_id = q.person_id AND i.identifier_type = 28 AND i.voided = 0)
                                    group by q.person_id

                          );

                           select CONCAT('Creating ndwr_all_patients_interim table ...');
                                      
						  
                          drop temporary table if exists ndwr_all_patients_interim;
                          
CREATE temporary TABLE ndwr_all_patients_interim (
    SELECT
    pm.Pkv as 'PKV',
    t1.person_id AS 'PatientPK',
    case
     when t1.visit_type in (23, 24, 119, 124, 129,43,80,118,120,123) THEN mfl2.mfl_code
     else mfl.mfl_code
    end as 'SiteCode',
    REPLACE(pm.PatientID, "-", "") AS 'PatientID',
    case
     when t1.visit_type in (23, 24, 119, 124, 129,43,80,118,120,123) THEN mfl2.mfl_code
     else mfl.mfl_code
    end as 'FacilityID',
    'AMRS' AS Emr,
    'Ampath Plus' AS 'Project',
    mfl.Facility AS FacilityName,
    pm.gender AS Gender,
    pm.birthdate AS DOB,
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
    END AS 'RegistrationDate',
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
    END AS 'RegistrationAtCCC',
    NULL AS 'RegistrationAtPMTCT',
    NULL AS 'RegistrationAtTBClinic',
    NULL AS 'PatientSource',
    l.state_province AS 'Region',
    NULL AS 'District',
    NULL AS 'Village',
    NULL AS 'ContactRelation',
	DATE(t1.encounter_datetime) AS 'LastVisit',
    NULL AS 'MaritalStatus',
    NULL AS 'EducationLevel',
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
    END AS 'DateConfirmedHIVPositive',
    NULL AS 'PreviousARTExposure',
    NULL AS 'PreviousARTStartDate',
   
    case
		when date_format(@endDate, "%Y-%m-01") > t1.death_date then @status := "dead"
		when date_format(@endDate, "%Y-%m-01") > date_format(transfer_out_date, "%Y-%m-01") then @status := "transfer_out"
		when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 28 day)),@endDate) <= 28 then @status := "active"
		when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 28 day)),@endDate) between 29 and 90 then @status := "defaulter"
		when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 28 day)),@endDate) > 90 then @status := "ltfu"
		else @status := "unknown"
	end as  'StatusAtCCC',
    NULL AS 'StatusAtPMTCT',
    NULL AS 'StatusAtTBClinic',
    NULL AS 'Inschool',
    IF(t1.arv_first_regimen_start_date,
        t1.arv_first_regimen_start_date,
        t1.enrollment_date) AS 'arv_first_regimen_start_date',
	t1.rtc_date as 'rtc_date',
    REPLACE(IF(t1.arv_first_regimen,
        etl.get_arv_names(t1.arv_first_regimen),
        'unknown'), "##", "+") AS 'arv_first_regimen',
    IF(t1.arv_first_regimen_start_date,
        t1.arv_first_regimen_start_date,
        t1.enrollment_date) AS arv_start_date,
    REPLACE(etl.get_arv_names(t1.cur_arv_meds), "##", "+") AS 'cur_arv_meds',
    t1.cur_arv_line_strict as 'cur_arv_line_strict',
    t1.cur_arv_line as 'cur_arv_line',
    NULL AS 'KeyPopulationType',
    NULL AS 'Orphan',
    NULL AS PatientResidentCounty,
    NULL AS PatientResidentLocation,
    NULL AS PatientResidentSubCounty,
    NULL AS PatientResidentSubLocation,
    NULL AS PatientResidentVillage,
    NULL AS PatientResidentWard,
    NULL AS 'PatientType',
    'GeneralPopulation' AS 'PopulationType',
    NULL AS 'TransferInDate',
    pm.Occupation AS 'Occupation',
    NULL AS 'DateCreated'
FROM
    etl.flat_hiv_summary_v15b t1
    INNER JOIN
    ndwr_all_patients_build_queue__0 t3 ON (t3.person_id = t1.person_id)
    left join 
      ndwr_patient_pkv_occcupation_mapping pm on (t1.person_id = pm.person_id)
        JOIN
    ndwr.mfl_codes mfl ON (mfl.location_id = t1.location_id)
       left join ndwr.mfl_codes mfl2 on (mfl2.location_id = t1.last_non_transit_location_id)
        JOIN
    amrs.location l ON (l.location_id = t1.location_id)
     JOIN
    amrs.location l2 ON (l2.location_id = t1.last_non_transit_location_id)
    
WHERE
    t1.is_clinical_encounter = 1
        AND t1.next_clinical_datetime_hiv IS NULL
ORDER BY t1.encounter_datetime DESC);

                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_all_patients_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_all_patients_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_all_patients_build_queue__0 t2 using (person_id);'); 
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
