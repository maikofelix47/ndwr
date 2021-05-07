DELIMITER $$
CREATE  PROCEDURE `build_ndwr_patient_ipt_extract`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_ipt_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_ipt_extract_v1.0";
                    set @query_type= query_type;
          
          
CREATE TABLE IF NOT EXISTS ndwr_patient_ipt_extract (
  `PatientPK` INT NOT NULL,
  `SiteCode` INT NOT NULL,
  `PatientID` VARCHAR NULL,
  `FacilityID` INT NOT NULL,
  `Emr` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `FacilityName` VARCHAR(50) NULL,
  `VisitID` INT NULL,
  `VisitDate` DATETIME NULL,
  `OnTBDrugs` BOOLEAN NULL,
  `OnIPT`  BOOLEAN NULL,
  `EverOnIPT` BOOLEAN NULL,
  `Cough` BOOLEAN NULL,
  `Fever`  BOOLEAN NULL,
  `NoticeableWeightLoss`  BOOLEAN NULL,
  `NightSweats`  BOOLEAN NULL,
  `Lethergy`  BOOLEAN NULL,
  `ICFActionTaken`  VARCHAR(100) NULL,
  `ChestXrayResults` VARCHAR(100) NULL,
  `SputumSmearResults` VARCHAR(100) NULL,
  `GeneExpertResults` VARCHAR(100) NULL,
  `TestResult`  VARCHAR(100) NULL,
  `TBClinicalDiagnosis`  VARCHAR(100) NULL,
  `ContactsInvited`  VARCHAR(10) NULL,
  `EvaluatedForIPT`  BOOLEAN NULL,
  `StartAntiTBs`  BOOLEAN NULL,
  `TBRxStartDate`  DATETIME NULL,
  `TBScreening`  VARCHAR(100) NULL,
  `IPTClientWorkUp`  VARCHAR(100) NULL,
  `StartIPT`  VARCHAR(10) NULL,
  `IndicationForIPT`  VARCHAR(100) NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY VisitID (VisitID),
   INDEX patient_date (PatientID , VisitDate),
   INDEX patient_pk_ipt (PatientPK),
   INDEX date_created (DateCreated),
   INDEX site_code_ipt (SiteCode)
   INDEX site_code_ipt_pk (SiteCode,PatientPK)
);
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_ipt_extract);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_patient_ipt_extract_temp_",queue_number);
                            set @queue_table = concat("ndwr_patient_ipt_extract_build_queue_",queue_number);                    												

										        SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_ipt_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_ipt_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
                            set @write_table = concat("ndwr_patient_ipt_extract_temp_",queue_number);
                            set @queue_table = "ndwr_patient_ipt_extract_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr_patient_ipt_extract_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_patient_ipt_extract_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b where date_created >= @last_update);

                      end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_ipt_extract_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_ipt_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;

                          SELECT CONCAT('Creating ndwr_patient ccc numbers');
                          drop  temporary table if exists ndwr_patient_ccc;
                          CREATE temporary TABLE ndwr_patient_ccc(
                          select 
                           q.person_id,
                           i.identifier as 'ccc_no'
                           from 
                           ndwr.ndwr_patient_ipt_extract_build_queue__0 q
                           left join amrs.patient_identifier i on (i.patient_id = q.person_id AND i.identifier_type = 28 AND i.voided = 0)
                           group by q.person_id
                          );


                          drop temporary table if exists ndwr_patient_ipt_extract_0;

                          
                          create temporary table if exists ndwr_patient_ipt_extract_0(
                              select
                              f.person_id,
                              f.encounter_id,
                              f.encounter_datetime,
                              f.location_id,
                              f.on_ipt,
                              f.on_tb_tx,
                              f.tb_tx_start_date,
                              t.tb_screening_result,
                              o.obs
                              from
                              etl.flat_hiv_summary_v15b f
                              join ndwr_patient_ipt_extract_build_queue__0 q on (q.person_id = f.person_id)
                              left join etl.flat_obs o on (o.encounter_id = f.encounter_id and o.person_id = f.person_id)
                          );

                          drop temporary table if exists ndwr_patient_ipt_extract_1;

                          set @prev_id = -1;
                          set @cur_id = -1;
                          set @ever_on_ipt = null;

                          create temporary table ndwr_patient_ipt_extract_1(
                              select 
                              @prev_id := @cur_id as prev_id,
                              @cur_id :=  b.person_id as cur_id,
                              b.person_id as 'PatientPK',
                              mfl.mfl_code as 'SiteCode',
                              c.ccc_no as 'PatientID',
                              mfl.mfl_code as 'FacilityID',
                              'AMRS' AS Emr,
						                  'Ampath Plus' AS 'Project',
                              mfl.Facility AS 'FacilityName',
                              b.encounter_id AS 'VisitID',
                              b.encounter_datetime AS 'VisitDate',
                              b.on_tb_tx as 'OnTBDrugs',
                              b.on_ipt as 'OnIPT',
                              CASE
                                   WHEN (@ever_on_ipt IS NULL OR @ever_on_ipt = 0) THEN @ever_on_ipt := b.on_ipt
                                   WHEN @prev_id != @cur_id THEN @ever_on_ipt:=NULL
                                   ELSE @ever_on_ipt
                              END as 'EverOnIPT',
                              CASE
                                WHEN b.obs REGEXP "!!6174=6171!!" THEN 1
                                ELSE NULL
                              END AS 'Cough',
                              CASE
                                WHEN b.obs REGEXP "!!6174=8065!!" THEN 1
                                ELSE NULL
                              END AS 'Fever',
                              CASE
                                WHEN b.obs REGEXP "!!6174=832!!" THEN 1
                                ELSE NULL
                              END AS 'NoticeableWeightLoss',
                              CASE
                                WHEN b.obs REGEXP "!!6174=8061!!" THEN 1
                                ELSE NULL
                              END AS 'NightSweats',
                              NULL AS 'Lethargy',
                              CONCAT(
                                IF( b.obs REGEXP "!!10304=",'Gene Expert | ',''),
                                IF(b.obs REGEXP "!!307=" ,'Sputum Smear | ',''),
                                IF(b.obs REGEXP "!!12=" ,'Chest Xray ','')
                                ) AS 'ICFActionTaken',
                             CASE
                                WHEN b.obs REGEXP "!!12=1115" THEN 'NORMAL'
                                WHEN b.obs REGEXP "!!12=1136" THEN 'PULMONARY EFFUSION'
                                WHEN b.obs REGEXP "!!12=1137" THEN 'MILIARY CHANGES'
                                WHEN b.obs REGEXP "!!12=5158" THEN 'EVIDENCE OF CARDIAC ENLARGEMENT'
                                WHEN b.obs REGEXP "!!12=6049" THEN 'INFILTRATE'
                                WHEN b.obs REGEXP "!!12=6050" THEN 'DIFFUSE NON-MILIARY CHANGES'
                                WHEN b.obs REGEXP "!!12=6052" THEN 'CAVITARY LESION'
                                WHEN b.obs REGEXP "!!12=1116" THEN 'ABNORMAL'
                                WHEN b.obs REGEXP "!!12=1118" THEN 'NOT DONE'
                                WHEN b.obs REGEXP "!!12=10765" THEN 'ABNORMAL CHEST X-RAY'
                                ELSE NULL
                              END AS 'ChestXrayResults',
                              CASE
                                WHEN b.obs REGEXP "!!307=1304" THEN 'POOR SAMPLE QUALITY'
                                WHEN b.obs REGEXP "!!307=664" THEN 'NEGATIVE'
                                WHEN b.obs REGEXP "!!307=703" THEN 'POSITIVE'
                                WHEN b.obs REGEXP "!!307=2303" THEN '3+'
                                WHEN b.obs REGEXP "!!307=2302" THEN '2+'
                                WHEN b.obs REGEXP "!!307=2301" THEN '1+'
                                WHEN b.obs REGEXP "!!307=1138" THEN 'INDETERMINATE'
                                WHEN b.obs REGEXP "!!307=1116" THEN 'ABNORMAL'
                                WHEN b.obs REGEXP "!!307=1118" THEN 'NOT DONE'
                                WHEN b.obs REGEXP "!!307=9047" THEN 'SCANTY'
                                ELSE NULL
                              END AS 'SputumSmearResults',
                              CASE
                                WHEN b.obs REGEXP "!!8070=1304" THEN 'POSITIVE'
                                WHEN b.obs REGEXP "!!8070=664" THEN 'NEGATIVE'
                                WHEN b.obs REGEXP "!!8070=1304" THEN 'POOR SAMPLE QUALITY'
                                WHEN b.obs REGEXP "!!8070=1138" THEN 'INDETERMINATE'
                                ELSE NULL
                              END AS 'GeneExpertResults',
                              NULL AS 'TBClinicalDiagnosis',
                              NULL AS 'ContactsInvited',
                              CASE
                                WHEN b.obs REGEXP "!!9742=1065" THEN 1
                                ELSE 0
                              END AS 'EvaluatedForIPT',
                              CASE
                                WHEN b.on_tb_tx = 1 THEN 1
                                WHEN b.on_tb_tx = 0 THEN 0
                                ELSE NULL
                              END AS 'StartAntiTBs',
                              CASE
                                WHEN b.on_tb_tx = 1 THEN 1
                                WHEN b.on_tb_tx = 0 THEN 0
                                ELSE NULL
                              END AS 'StartAntiTBs',
                              b.tb_tx_start_date as 'TBRxStartDate',
                              CASE
                              WHEN b.obs REGEXP "!!8292=1107" THEN 'No Signs'
                              WHEN b.obs REGEXP "!!8292=6176" THEN 'On TB treatment'
                              WHEN b.obs REGEXP "!!8292=6971" THEN 'Suspect'
                              WHEN b.obs REGEXP "!!8292=6137" THEN 'Confirmed'
                              WHEN b.obs REGEXP "!!8292=1118" THEN 'Not assessed'
                              END AS 'TBScreening',
                              NULL AS 'IPTClientWorkUp',
                              NULL AS 'StartIPT',
                              NULL AS 'IndicationForIPT'
                              from
                              ndwr_patient_ipt_extract_0 b
                               JOIN
                              ndwr.mfl_codes mfl ON (mfl.location_id = b.location_id)
                              left join ndwr_patient_ccc c on (c.person_id = b.person_id )
                              order by b.encounter_datetime asc
                          );

                          alter table ndwr_patient_ipt_extract_1 drop prev_id, drop cur_id;
						  
                          drop temporary table if exists ndwr_patient_ipt_extract_interim;
                          
                          create temporary table ndwr_patient_ipt_extract_interim(
                            SELECT
                              PatientPK,
                              SiteCode,
                              PatientID,
                              FacilityID,
                              Emr,
						                  Project,
                              FacilityName,
                              VisitID,
                              VisitDate,
                              OnTBDrugs,
                              OnIPT,
                              EverOnIPT,
                              Cough,
                              Fever,
                              NoticeableWeightLoss,
                              NightSweats,
                              Lethargy,
                              ICFActionTaken,
                              ChestXrayResults,
                              SputumSmearResults,
                              GeneExpertResults,
                              CONCAT(IF(ChestXrayResults is not null,CONCAT('Chest XRay :',ChestXrayResults),'' ),
                              IF(SputumSmearResults is not null,CONCAT('Sputum Smear :',SputumSmearResults),'' ),
                              IF(GeneExpertResults is not null,CONCAT('Chest XRay :',GeneExpertResults),'' )) AS 'TesResult',
                              TBClinicalDiagnosis,
                              ContactsInvited,
                              EvaluatedForIPT,
                              StartAntiTBs,
                              StartAntiTBs,
                              TBRxStartDate,
                              TBScreening,
                              IPTClientWorkUp,
                              StartIPT,
                              IndicationForIPT
                            from
                            ndwr_patient_ipt_extract_1

                          );
                          
						 SELECT CONCAT('Creating interim table');

                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_ipt_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_ipt_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_ipt_extract_build_queue__0 t2 using (person_id);'); 
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
