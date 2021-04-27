DELIMITER $$
CREATE  PROCEDURE `build_ndwr_patient_baselines_extract`(IN query_type varchar(50) ,IN queue_number int, IN queue_size int, IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_patient_baselines_extract";
          set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_patient_baselines_extract_v1.0";
          set @query_type= query_type;
          set @cycle_size = 1;
          
          
CREATE TABLE IF NOT EXISTS `ndwr`.`ndwr_patient_baselines_extract` (
  `PatientPK` INT NOT NULL,
  `PatientID` INT NOT NULL,
  `FacilityID` INT NULL,
  `SiteCode` INT NOT NULL,
  `EMR` VARCHAR(50) NULL,
  `Project` VARCHAR(50) NULL,
  `bCD4` INT NULL,
  `bCD4Date` DATETIME NULL,
  `bWAB` INT NULL,
  `bWABDate` DATETIME NULL,
  `bWHO` INT NULL,
  `bWHODate` DATETIME NULL,
  `eWAB` INT NULL,
  `eWABDate` DATETIME NULL,
  `eCD4` INT NULL,
  `eCD4Date` DATETIME NULL,
  `eWHO` INT NULL,
  `eWHODate` DATETIME NULL,
  `lastWHO` INT NULL,
  `lastWHODate` DATETIME NULL,
  `lastCD4` INT NULL,
  `lastCD4Date` DATETIME NULL,
  `lastWAB` INT NULL,
  `lastWABDate` DATETIME NULL,
  `m12CD4` INT NULL,
  `m12CD4Date` DATETIME NULL,
  `m6CD4` INT NULL,
  `m6CD4Date` DATETIME NULL,
  `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	 INDEX baseline_patient_id (PatientID),
   INDEX baseline_patient_pk (PatientPK),
   INDEX baseline_facility_id (FacilityID),
   INDEX baselinne_site_code (SiteCode),
   INDEX baseline_date_created (DateCreated),
   INDEX baseline_patient_facility (PatientID,FacilityID)
  );

	                  set @last_date_created = (select max(DateCreated) from ndwr.ndwr_patient_baselines_extract);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_patient_baselines_extract_temp_",queue_number);
                            set @queue_table = concat("ndwr_patient_baselines_extract_build_queue_",queue_number);                    												

										  			SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_patient_baselines_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_patient_baselines_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
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
                            set @write_table = concat("ndwr_patient_baselines_extract_temp_",queue_number);
                            set @queue_table = "ndwr_patient_baselines_extract_sync_queue";
                            CREATE TABLE IF NOT EXISTS ndwr_patient_baselines_extract_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                            
                            set @last_update = null;
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                ndwr.flat_log
                            WHERE
                                table_name = @table_version;

                            replace into ndwr_patient_baselines_extract_sync_queue
                             (select distinct person_id from etl.flat_hiv_summary_v15b where date_created >= @last_update);

														replace into ndwr_patient_baselines_extract_sync_queue
                             (select distinct person_id from etl.flat_lab_obs where date_created >= @last_update);

                      end if;

                    set @total_time=0;
                    set @cycle_number = 0;

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop temporary table if exists ndwr_patient_baselines_extract_build_queue__0;

                          SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_patient_baselines_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',@cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;
                                      
						  
                drop temporary table if exists ndwr_base_line_0;
                          
                SELECT CONCAT("Writing to ndwr_base_line_0 ..");
                         
   							create temporary table ndwr_base_line_0(			
   							SELECT 
   							distinct
   							s.person_id,
   							l.location_id as location_id,
   							l.test_datetime AS test_datetime,
   							if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date) as arv_first_regimen_start_date,
                              s.cur_who_stage,
   							DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   								INTERVAL 12 MONTH) AS after_12,
   							DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   								INTERVAL 6 MONTH) AS after_6,
   							DATEDIFF(l.test_datetime,
   									if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date)) AS close_first_regimen_start,
   							SIGN(DATEDIFF(l.test_datetime,
   											if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date))) lab_art_start_comparison,
   							ABS(DATEDIFF(l.test_datetime,
   											DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   												INTERVAL 6 MONTH))) AS test_close_to_6,
   							ABS(DATEDIFF(l.test_datetime,
   											DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   												INTERVAL 12 MONTH))) AS test_close_to_12,
   							IF(l.obs REGEXP '!!5497=[0-9]',
   								CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(l.obs,
   														LOCATE('!!5497=', l.obs)),
   													'##',
   													1)),
   											'!!5497=',
   											''),
   										'!!',
   										'')
   									AS UNSIGNED),
   								NULL) AS cd4,
   							IF(l.obs REGEXP '!!730=[0-9]',
   								CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(l.obs,
   														LOCATE('!!730=', l.obs)),
   													'##',
   													1)),
   											'!!730=',
   											''),
   										'!!',
   										'')
   									AS UNSIGNED),
   								NULL) AS cd4_percent,
   							IF(l.obs REGEXP '!!856=[0-9]',
   								CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(l.obs,
   														LOCATE('!!856=', l.obs)),
   													'##',
   													1)),
   											'!!856=',
   											''),
   										'!!',
   										'')
   									AS UNSIGNED),
   								NULL) AS v_l
                               FROM
                                   etl.flat_hiv_summary_v15b s
                                   LEFT JOIN
                                   etl.flat_lab_obs l USING (person_id)
                                   inner join ndwr_patient_baselines_extract_build_queue__0 b on (b.person_id = s.person_id)
  								 where	(s.arv_first_regimen_start_date <> ''	or s.enrollment_date<>'')			
                 );

                  set @lastCd4 := null;	
                  set @lastCd4_date := null;		
                  set @lastCd4Pc := null;		
                  set @last_cd4_percent_date := null;
                  set @cd4_percent_date_before_arv_start := null ;
                  set @cd4_percent_before_arv_start := null ;
                  set @cd4_percent_date_after_arv_start := null ;
                  set @cd4_percent_after_arv_start := null ;
                  set @cd4_date_after_arv_start := null ;
                  set @cd4_after_arv_start := null ;
                  set @cd4_date_before_arv_start := null ;
                  set @cd4_percent_after_12month_arv := null ;
                  set @cd4_percent_date_after_12month_arv := null ;
                  set @cd4_after_12month_arv := null ;
                  set @cd4_date_after_12month_arv := null ;
                  set @cd4_after_6month_arv := null ;
                  set @cd4_date_after_6month_arv := null ;
                  set @cd4_percent_date_after_6month_arv := null ;
                  set @cd4_percent_after_6month_arv := null ;
                  set @cd4_percent_date_after_arv_start := null ;
                  set @cd4_before_arv_start := null ;
                  set @cd4_percent_at_arv_start := null ;
                  set @cd4_at_arv_start_date := null ;
                  set @cd4_at_arv_start := null ;
                  set @cd4_percent_at_arv_start_date  := null ;	

                  drop temporary table if exists base_temp_1;
                  create temporary table base_temp_1(
 		              SELECT  distinct t1.person_id,
 
 							case							   
 								when  t1.cd4<>'' then @lastCd4 := cd4
 								when  @lastCd4 is null	then @lastCd4 := t1.cd4
 								else  @lastCd4
 							end as    lastCd4,
 							case							   
 								when  t1.cd4<>'' then @lastCd4_date := t1.test_datetime
 								when  @lastCd4_date is null	then @lastCd4_date := t1.test_datetime
 								else  @lastCd4_date
 							end as    lastCd4_date,
 							case							   
 								when  t1.cur_who_stage<>'' then @cur_who_stage := t1.cur_who_stage
 								when  @cur_who_stage is null	then @cur_who_stage := t1.cur_who_stage
 								else  @cur_who_stage
 							end as    last_who_stage,
 							case							   
 								when  t1.cur_who_stage<>'' then @last_who_stage_date := t1.test_datetime
 								when  @last_who_stage_date is null	then @last_who_stage_date := t1.test_datetime
 								else  @last_who_stage_date
 							end as    last_who_stage_date,
 							case
 								when t1.cd4_percent<>'' 
 								then @lastCd4Pc := t1.cd4_percent
 								when @lastCd4Pc is null
 								then @lastCd4Pc := t1.cd4_percent
 								else @lastCd4Pc
 							end as last_cd4_percent,
 							case
 								when t1.cd4_percent<>''	then @last_cd4_percent_date := t1.test_datetime
 								when  @last_cd4_percent_date is null then @last_cd4_percent_date := t1.test_datetime 
 								else @last_cd4_percent_date
 							end as last_cd4_percent_date,							
 							case
 								when  t1.close_first_regimen_start=0 
 								then @cd4_percent_at_arv_start := t1.cd4_percent
 								when @cd4_percent_at_arv_start is null
 								then @cd4_percent_at_arv_start := t1.cd4_percent
 								else @cd4_percent_at_arv_start
 							end as cd4_percent_at_arv_start,
 							case 
 								when  t1.close_first_regimen_start=0 
 								then @cd4_percent_at_arv_start_date := t1.test_datetime
 								when @cd4_percent_at_arv_start_date is null
 								then @cd4_percent_at_arv_start_date := t1.test_datetime
 								else @cd4_percent_at_arv_start_date
 							end as cd4_percent_at_arv_start_date,
 							case
 								when  t1.close_first_regimen_start=0 
 								then @cd4_at_arv_start := t1.cd4
 								when @cd4_at_arv_start is null
 								then @cd4_at_arv_start := t1.cd4
 								else @cd4_at_arv_start
 							end as cd4_at_arv_start,
 							case
 								when  t1.close_first_regimen_start=0 
 								then @cd4_at_arv_start_date := t1.test_datetime
 								when @cd4_at_arv_start_date is null
 								then @cd4_at_arv_start_date := t1.test_datetime
 								else @cd4_at_arv_start_date
 							end as cd4_at_arv_start_date,
 							case
 								when  t1.close_first_regimen_start=0 
 								then @who_stage_at_arv_start := t1.cur_who_stage
 								when @who_stage_at_arv_start is null
 								then @who_stage_at_arv_start := t1.cur_who_stage
 								else @who_stage_at_arv_start
 							end as who_stage_at_arv_start,
 							case
 								when  t1.close_first_regimen_start=0 
 								then @who_stage_at_arv_start_date := t1.test_datetime
 								when @who_stage_at_arv_start_date is null
 								then @who_stage_at_arv_start_date := t1.test_datetime
 								else @who_stage_at_arv_start_date
 							end as who_stage_at_arv_start_date,							
 						    case
 								when  sign(t1.close_first_regimen_start)=-1 
 								then @cd4_before_arv_start := t1.cd4
 								when @cd4_before_arv_start is null and sign(t1.close_first_regimen_start)=-1
 								then @cd4_before_arv_start := t1.cd4
 								else @cd4_before_arv_start
 							end as cd4_before_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=-1 
 								then @cd4_date_before_arv_start := t1.test_datetime
 								when @cd4_date_before_arv_start is null and sign(t1.close_first_regimen_start)=-1
 								then @cd4_date_before_arv_start := t1.test_datetime
 								else @cd4_date_before_arv_start
 							end as cd4_date_before_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=-1 
 								then @cur_who_stage_before_arv_start := t1.cur_who_stage
 								when @cur_who_stage_before_arv_start is null and sign(t1.close_first_regimen_start)=-1
 								then @cur_who_stage_before_arv_start := t1.cur_who_stage
 								else @cur_who_stage_before_arv_start
 							end as cur_who_stage_before_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=-1 
 								then @cur_who_stage_date_before_arv_start := t1.test_datetime
 								when @cur_who_stage_date_before_arv_start is null and sign(t1.close_first_regimen_start)=-1
 								then @cur_who_stage_date_before_arv_start := t1.test_datetime
 								else @cur_who_stage_date_before_arv_start
 							end as cur_who_stage_date_before_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=-1 
 								then @cd4_percent_before_arv_start := t1.cd4_percent
 								when @cd4_percent_before_arv_start is null and sign(t1.close_first_regimen_start)=-1
 								then @cd4_percent_before_arv_start := t1.cd4_percent
 								else @cd4_percent_before_arv_start
 							end as cd4_percent_before_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=-1 
 								then @cd4_percent_date_before_arv_start := t1.test_datetime
 								when @cd4_percent_date_before_arv_start is null and sign(t1.close_first_regimen_start)=-1
 								then @cd4_percent_date_before_arv_start := t1.test_datetime
 								else @cd4_percent_date_before_arv_start
 							end as cd4_percent_date_before_arv_start,							
 							case
 								when  sign(t1.close_first_regimen_start)=1 and t1.close_first_regimen_start>0 
 								then @cd4_after_arv_start := t1.cd4
 								when t1.close_first_regimen_start>0  and sign(t1.close_first_regimen_start)=1
 								then @cd4_after_arv_start := t1.cd4
 								else @cd4_after_arv_start
 							end as cd4_after_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=1 and t1.close_first_regimen_start>0 
 								then @cd4_date_after_arv_start := t1.test_datetime
 								when t1.close_first_regimen_start>0  and sign(t1.close_first_regimen_start)=1
 								then @cd4_date_after_arv_start := t1.test_datetime
 								else @cd4_date_after_arv_start
 							end as cd4_date_after_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=1 and t1.close_first_regimen_start>0 
 								then @cd4_percent_after_arv_start := t1.cd4_percent
 								when t1.close_first_regimen_start>0  and sign(t1.close_first_regimen_start)=1
 								then @cd4_percent_after_arv_start := t1.cd4_percent
 								else @cd4_percent_after_arv_start
 							end as cd4_percent_after_arv_start,
 							case
 								when  sign(t1.close_first_regimen_start)=1 and t1.close_first_regimen_start>0 
 								then @cd4_percent_date_after_arv_start := t1.test_datetime
 								when t1.close_first_regimen_start>0  and sign(t1.close_first_regimen_start)=1
 								then @cd4_percent_date_after_arv_start := t1.test_datetime
 								when @cd4_percent_date_after_arv_start is null
 								then @cd4_percent_date_after_arv_start := t1.test_datetime
 								else @cd4_percent_date_after_arv_start
 							end as cd4_percent_date_after_arv_start,
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @cur_who_stage_after_6month_arv := t1.cur_who_stage
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @cur_who_stage_after_6month_arv := t1.cur_who_stage
 								when @cur_who_stage_after_6month_arv is null
 								then @cur_who_stage_after_6month_arv := t1.cur_who_stage
 								else @cur_who_stage_after_6month_arv
 							end as cur_who_stage_after_6month_arv,
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @cur_who_stage_date_after_6month_arv := t1.test_datetime
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @cur_who_stage_date_after_6month_arv := t1.test_datetime
 								when @cur_who_stage_date_after_6month_arv is null
 								then @cur_who_stage_date_after_6month_arv := t1.test_datetime
 								else @cur_who_stage_date_after_6month_arv
 							end as cur_who_stage_date_after_6month_arv,
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @v_l_after_6month_arv := t1.v_l
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @v_l_after_6month_arv := t1.v_l
 								when @v_l_after_6month_arv is null
 								then @v_l_after_6month_arv := t1.v_l
 								else @v_l_after_6month_arv
 							end as v_l_after_6month_arv,
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @v_l_date_after_6month_arv := t1.test_datetime
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @v_l_date_after_6month_arv := t1.test_datetime
 								when @v_l_date_after_6month_arv is null
 								then @v_l_date_after_6month_arv := t1.test_datetime
 								else @v_l_date_after_6month_arv
 							end as v_l_date_after_6month_arv,	
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @cd4_percent_after_6month_arv := t1.cd4_percent
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @cd4_percent_after_6month_arv := t1.cd4_percent
 								when @cd4_percent_after_6month_arv is null
 								then @cd4_percent_after_6month_arv := t1.cd4_percent
 								else @cd4_percent_after_6month_arv
 							end as cd4_percent_after_6month_arv,
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @cd4_percent_date_after_6month_arv := t1.test_datetime
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @cd4_percent_date_after_6month_arv := t1.test_datetime
 								when @cd4_percent_date_after_6month_arv is null
 								then @cd4_percent_date_after_6month_arv := t1.test_datetime
 								else @cd4_percent_date_after_6month_arv
 							end as cd4_percent_date_after_6month_arv,				
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @cd4_after_6month_arv := t1.cd4
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @cd4_after_6month_arv := t1.cd4
 								when @cd4_after_6month_arv is null
 								then @cd4_after_6month_arv := t1.cd4
 								else @cd4_after_6month_arv
 							end as cd4_after_6month_arv,
 							case
 								when  sign(t1.test_close_to_6)=1 and t1.test_close_to_6>0 
 								then @cd4_date_after_6month_arv := t1.test_datetime
 								when t1.test_close_to_6>0  and sign(t1.test_close_to_6)=1
 								then @cd4_date_after_6month_arv := t1.test_datetime
 								when @cd4_date_after_6month_arv is null
 								then @cd4_date_after_6month_arv := t1.test_datetime
 								else @cd4_date_after_6month_arv
 							end as cd4_date_after_6month_arv,
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @cur_who_stage_after_12month_arv := t1.cur_who_stage
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @cur_who_stage_after_12month_arv := t1.cur_who_stage
 								when @cur_who_stage_after_12month_arv is null
 								then @cur_who_stage_after_12month_arv := t1.cur_who_stage
 								else @cur_who_stage_after_12month_arv
 							end as cur_who_stage_after_12month_arv,							
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @cur_who_stage_date_after_12month_arv := t1.test_datetime
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @cur_who_stage_date_after_12month_arv := t1.test_datetime
 								when  @cur_who_stage_date_after_12month_arv is null
 								then @cur_who_stage_date_after_12month_arv := t1.test_datetime
 								else @cur_who_stage_date_after_12month_arv
 							end as cur_who_stage_date_after_12month_arv ,
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @cd4_percent_after_12month_arv := t1.cd4_percent
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @cd4_percent_after_12month_arv := t1.cd4_percent
 								when @cd4_percent_after_12month_arv is null
 								then @cd4_percent_after_12month_arv := t1.cd4_percent
 								else @cd4_percent_after_12month_arv
 							end as cd4_percent_after_12month_arv,							
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @cd4_percent_date_after_12month_arv := t1.test_datetime
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @cd4_percent_date_after_12month_arv := t1.test_datetime
 								when  @cd4_percent_date_after_12month_arv is null
 								then @cd4_percent_date_after_12month_arv := t1.test_datetime
 								else @cd4_percent_date_after_12month_arv
 							end as cd4_percent_date_after_12month_arv ,
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @cd4_after_12month_arv := t1.cd4
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @cd4_after_12month_arv := t1.cd4
 								when @cd4_after_12month_arv is null
 								then @cd4_after_12month_arv := t1.cd4
 								else @cd4_after_12month_arv 
 							end as cd4_after_12month_arv,
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @cd4_date_after_12month_arv := t1.test_datetime
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @cd4_date_after_12month_arv := t1.test_datetime
 								when @cd4_date_after_12month_arv is null 
 								then @cd4_date_after_12month_arv := t1.test_datetime
 								else @cd4_date_after_12month_arv
 							end as cd4_date_after_12month_arv,
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @v_l_after_12month_arv := t1.v_l
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @v_l_after_12month_arv := t1.v_l
 								when @v_l_after_12month_arv is null
 								then @v_l_after_12month_arv := t1.v_l
 								else @v_l_after_12month_arv
 							end as v_l_after_12month_arv,							
 							case
 								when  sign(t1.test_close_to_12)=1 and t1.test_close_to_12>0 
 								then @v_l_date_after_12month_arv := t1.test_datetime
 								when t1.test_close_to_12>0  and sign(t1.test_close_to_12)=1
 								then @v_l_date_after_12month_arv := t1.test_datetime
 								when  @v_l_date_after_12month_arv is null
 								then @v_l_date_after_12month_arv := t1.test_datetime
 								else @v_l_date_after_12month_arv
 							end as v_l_date_after_12month_arv 
 							
 							
                 FROM ndwr_base_line_0 t1 
                 inner join ndwr_patient_baselines_extract_build_queue__0 b on (b.person_id = t1.person_id)
                 order by close_first_regimen_start desc
                  );

              drop temporary table if exists patient_base_line;
                          
			  CREATE temporary TABLE  if not exists patient_base_line(
 							  select
                 b.person_id as person_id,
                 @lastCd4 as lastCd4,
                 @lastCd4_date as lastCd4_date,
                 @last_who_stage as last_who_stage,
                 @last_who_stage_date as last_who_stage_date,
                 @last_cd4_percent as last_cd4_percent,
                 @last_cd4_percent_date as last_cd4_percent_date,
                 @cd4_percent_at_arv_start as cd4_percent_at_arv_start,
                 @cd4_at_arv_start_date as cd4_percent_at_arv_start_date,
                 @cd4_at_arv_start as cd4_at_arv_start,
                 @cd4_at_arv_start_date as cd4_at_arv_start_date,
                 @who_stage_at_arv_start as who_stage_at_arv_start,
                 @who_stage_at_arv_start_date as who_stage_at_arv_start_date,
                 @cd4_before_arv_start as cd4_before_arv_start,
                 @cd4_date_before_arv_start as cd4_date_before_arv_start,
                 @cur_who_stage_before_arv_start as cur_who_stage_before_arv_start,
                 @cur_who_stage_date_before_arv_start cur_who_stage_date_before_arv_start,
                 @cd4_percent_before_arv_start as cd4_percent_before_arv_start,
                 @cd4_percent_date_before_arv_start as cd4_percent_date_before_arv_start,
                 @cd4_after_arv_start as cd4_after_arv_start,
                 @cd4_date_after_arv_start as cd4_date_after_arv_start,
                 @cd4_percent_after_arv_start as cd4_percent_after_arv_start,
                 @cd4_percent_date_after_arv_start as cd4_percent_date_after_arv_start,
                 @cur_who_stage_after_6month_arv as cur_who_stage_after_6month_arv,
                 @cur_who_stage_date_after_6month_arv as cur_who_stage_date_after_6month_arv,
                 @v_l_after_6month_arv as v_l_after_6month_arv,
                 @v_l_date_after_6month_arv as v_l_date_after_6month_arv,
                 @cd4_percent_after_6month_arv as  cd4_percent_after_6month_arv,
                 @cd4_percent_date_after_6month_arv as cd4_percent_date_after_6month_arv,
                 @cd4_after_6month_arv as cd4_after_6month_arv,
                 @cd4_date_after_6month_arv as cd4_date_after_6month_arv,
                 @cur_who_stage_after_12month_arv as cur_who_stage_after_12month_arv,
                 @cur_who_stage_date_after_12month_arv as cur_who_stage_date_after_12month_arv,
                 @cd4_percent_after_12month_arv as cd4_percent_after_12month_arv,
                 @cd4_percent_date_after_12month_arv as cd4_percent_date_after_12month_arv,
                 @cd4_after_12month_arv as cd4_after_12month_arv,
                 @cd4_date_after_12month_arv as cd4_date_after_12month_arv,
                 @v_l_after_12month_arv as v_l_after_12month_arv,
                 @v_l_date_after_12month_arv as v_l_date_after_12month_arv
                 from 
                 ndwr_patient_baselines_extract_build_queue__0 b
                 );		
 							
        drop temporary table if exists ndwr_base_line;
        CREATE temporary TABLE ndwr_base_line (
        SELECT
                         b.person_id as PatientPK,
                         b.person_id as PatientID,
                         t1.FacilityName AS FacilityName,
                         t1.FacilityID as LocationID,
                         t1.FacilityID as FacilityID,
                         t1.SiteCode as SiteCode,
                         b.cd4_before_arv_start as bCD4,
                         b.cd4_date_before_arv_start as bCD4Date,
                         b.cur_who_stage_before_arv_start as bWHO,
                         b.cur_who_stage_date_before_arv_start as bWHODate,
                         b.cd4_at_arv_start as eCD4,
                         b.cd4_at_arv_start_date as eCD4Date,
                         b.who_stage_at_arv_start as eWHO,
                         b.who_stage_at_arv_start_date as eWHODate,
                         b.last_who_stage as lastWHO,
                         b.last_who_stage_date as lastWHODate,
                         b.lastCd4 as lastCD4,
                         b.lastCd4_date as lastCD4Date,
                         b.cd4_after_12month_arv as m12CD4,
                         b.cd4_date_after_12month_arv as m12CD4Date,
                         b.cd4_after_6month_arv as m6CD4,
                         b.cd4_date_after_6month_arv as m6CD4Date,
                         b.cd4_at_arv_start as CD4atEnrollment,
                         b.cd4_at_arv_start_date as CD4atEnrollment_Date,
                         b.cd4_before_arv_start as CD4BeforeARTStart,
                         b.cd4_date_before_arv_start as CD4BeforeARTStart_Date,
                         b.lastCd4 as LastCD4AfterARTStart,
                         b.lastCd4_date as LastCD4AfterARTStart_Date,
                         b.cd4_percent_at_arv_start as CD4atEnrollmentPercent,
                         b.cd4_percent_at_arv_start_date as CD4atEnrollmentPercent_Date,
                         b.cd4_percent_before_arv_start as CD4BeforeARTStartPercent,
                         b.cd4_percent_date_before_arv_start as CD4BeforeARTStartPercent_Date,
                         b.last_cd4_percent as LastCD4AfterARTStartPercent,
                         b.last_cd4_percent_date as LastCD4AfterARTStartPercent_Date,
                         b.cd4_after_6month_arv as `sixMonthCD4`,
                         b.cd4_date_after_6month_arv as `sixMonthCD4_Date`,
                         b.cd4_after_12month_arv as `twelveMonthCD4`,
                         b.cd4_date_after_12month_arv as `twelveMonthCD4_Date`,
                         b.cd4_percent_after_12month_arv as `sixMonthCD4Percent`,
                         b.cd4_percent_date_after_12month_arv as `sixMonthCD4Percent_Date`,
                         b.cd4_after_arv_start as FirstCD4AfterARTStart,
                         b.cd4_date_after_arv_start as FirstCD4AfterARTStart_Date,
                         b.cd4_percent_after_arv_start as FirtsCD4AfterARTStartPercent,
                         b.cd4_percent_date_after_arv_start as FirtsCD4AfterARTStartPercent_date,
                         null as Imported,
                         b.v_l_after_6month_arv as `sixMonthVL`,
                         b.v_l_date_after_6month_arv as `sixMonthVlDate`,
                         b.v_l_after_12month_arv as `twelveMonthVL`,
                         b.v_l_date_after_12month_arv as `twelveMonthVLDate`,
                         if(b.lastCd4_date,b.lastCd4_date,b.v_l_date_after_12month_arv) as VisitDate
						 from patient_base_line b
						 inner join ndwr.ndwr_all_patients_extract t1 on (t1.PatientID = b.person_id)
						 inner join ndwr_patient_baselines_extract_build_queue__0 q on (q.person_id = b.person_id)
                                 );

            SELECT CONCAT('Creating interim table');

            drop temporary table if exists ndwr_patient_baselines_extract_interim;

            CREATE temporary TABLE ndwr_patient_baselines_extract_interim (
                  select 
                     b.PatientPK,
                     b.PatientID,
                     b.SiteCode as FacilityId,
                     b.SiteCode,
                     'AMRS' as EMR,
                     'Ampath' as Project,
					 b.bCD4,
                     b.bCD4Date,
                     null as bWAB,
                     null as bWABDate,
                     b.bWHO,
                     b.bWHODate,
                     null as eWAB,
                     null as eWABDate,
					 b.eCD4,
					 b.eCD4Date,
                     b.eWHO,
					 b.eWHODate,
                     b.lastWHO,
					 b.lastWHODate,
                     b.lastCD4,
					 b.lastCD4Date,
                     null as lastWAB,
                     null as lastWABDate,
                     b.twelveMonthCD4 as m12CD4,
                     b.twelveMonthCD4_Date as m12CD4Date,
                     b.sixMonthCD4 as m6CD4,
                     b.sixMonthCD4_Date as m6CD4Date,
                     null as DateCreated
				           from 
                   ndwr.ndwr_base_line b
                   inner join ndwr_patient_baselines_extract_build_queue__0 q on (b.PatientID = q.person_id)
                );


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_patient_baselines_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_patient_baselines_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_patient_baselines_extract_build_queue__0 t2 using (person_id);'); 
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
                         set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / @cycle_size) / 60);
                         
SELECT 
    @person_ids_count AS 'persons remaining',
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / @cycle_size) AS remaining_cycles,
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
