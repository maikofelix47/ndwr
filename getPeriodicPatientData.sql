CREATE  PROCEDURE `getPeriodicPatientData`(
IN startDate Date,
IN endDate Date
)
BEGIN

 set @startDate:=startDate;
 set @endDate:=endDate;
 
delete from ndwr.patint_current_data_2 ;
delete from ndwr.ltfu_details_2;
delete from ndwr.patint_initial_data_2 ;
delete from ndwr.patient_last_followup_data_2;
delete from ndwr.patient_current_art_data;

 replace into ndwr.patient_current_art_data
SELECT DISTINCT
        person_id,
            last_vl_1,
            last_vl_1_date,
            vl_2,
            vl_2_date,
            cd4_1_latest,
            cd4_1_date_latest,
            enrollment_date,
            arv_first_regimen_start_date,
            cur_arv_adherence,
            cur_arv_meds,
            cur_who_stage,
            last_location ,
            last_encounter_type
    FROM 
        (SELECT
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
            
            CASE
                WHEN @prev_id != @cur_id AND t1.vl_1 THEN @vl_1:=t1.vl_1
                WHEN
                    @prev_id != @cur_id
                        AND (t1.vl_1 IS NULL OR t1.vl_1 = 0)
                THEN
                    @vl_1:=t1.vl_1
                WHEN
                    @prev_id = @cur_id AND t1.vl_1
                        AND @vl_1 IS NULL
                THEN
                    @vl_1:=t1.vl_1
                ELSE @vl_1
            END AS last_vl_1,
            CASE
                WHEN @prev_id != @cur_id AND t1.vl_1_date THEN @vl_1_date:=t1.vl_1_date
                WHEN
                    @prev_id != @cur_id
                        AND t1.vl_1_date IS NULL
                THEN
                    @vl_1_date:=t1.vl_1_date
                WHEN
                    @prev_id = @cur_id AND t1.vl_1_date
                        AND @vl_1_date IS NULL
                THEN
                    @vl_1_date:=t1.vl_1_date
                ELSE @vl_1_date
            END AS last_vl_1_date,  
            CASE
                WHEN @prev_id != @cur_id AND t1.vl_2 THEN @vl_2:=t1.vl_2
                WHEN
                    @prev_id != @cur_id
                        AND (t1.vl_2 IS NULL OR t1.vl_2 = 0)
                THEN
                    @vl_2:=t1.vl_2
                WHEN
                    @prev_id = @cur_id AND t1.vl_2
                        AND @vl_2 IS NULL
                THEN
                    @vl_2:=t1.vl_2
                ELSE @vl_2
            END AS vl_2,
                        CASE
                WHEN @prev_id != @cur_id AND t1.vl_2_date THEN @vl_2_date:=t1.vl_2_date
                WHEN
                    @prev_id != @cur_id
                        AND t1.vl_2_date IS NULL
                THEN
                    @vl_2_date:=t1.vl_2_date
                WHEN
                    @prev_id = @cur_id AND t1.vl_2_date
                        AND @vl_2_date IS NULL
                THEN
                    @vl_2_date:=t1.vl_2_date
                ELSE @vl_2_date
            END AS vl_2_date,
                        CASE
                WHEN @prev_id != @cur_id AND t1.cd4_1 THEN @cd4_1:=t1.cd4_1
                WHEN
                    @prev_id != @cur_id
                        AND (t1.cd4_1 IS NULL OR t1.cd4_1 = 0)
                THEN
                    @cd4_1:=t1.cd4_1
                WHEN
                    @prev_id = @cur_id AND t1.cd4_1
                        AND @cd4_1 IS NULL
                THEN
                    @cd4_1:=t1.cd4_1
                ELSE @cd4_1
            END AS cd4_1_latest,         
            CASE
                WHEN @prev_id != @cur_id AND t1.cd4_1_date THEN @cd4_1_date:=t1.cd4_1_date
                WHEN
                    @prev_id != @cur_id
                        AND (t1.cd4_1_date IS NULL OR t1.cd4_1_date = 0)
                THEN
                    @cd4_1_date:=t1.cd4_1_date
                WHEN
                    @prev_id = @cur_id AND t1.cd4_1_date
                        AND @cd4_1_date IS NULL
                THEN
                    @cd4_1_date:=t1.cd4_1_date
                ELSE @cd4_1_date
            END AS cd4_1_date_latest,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_adherence IS NOT NULL
                THEN
                    @cur_arv_adherence:=t1.cur_arv_adherence
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_adherence IS NULL
                THEN
                    @cur_arv_adherence:=t1.cur_arv_adherence
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_arv_adherence IS NOT NULL
                        AND @cur_arv_adherence IS NULL
                THEN
                    @cur_arv_adherence:=t1.cur_arv_adherence
                ELSE @cur_arv_adherence
            END AS cur_arv_adherence,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_meds IS NOT NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_meds IS NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_arv_meds IS NOT NULL
                        AND @cur_arv_meds IS NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                ELSE @cur_arv_meds
            END AS cur_arv_meds,
                      CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_who_stage IS NOT NULL
                THEN
                    @cur_who_stage:=t1.cur_who_stage
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_who_stage IS NULL
                THEN
                    @cur_who_stage:=t1.cur_who_stage
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_who_stage IS NOT NULL
                        AND @cur_who_stage IS NULL
                THEN
                    @cur_who_stage:=t1.cur_who_stage
                ELSE @cur_who_stage
            END AS cur_who_stage,
 CASE
                
				
				WHEN
                    @prev_id != @cur_id
                        AND t1.location_id IS NOT NULL
                THEN
                    @location_id:=t1.location_id
                WHEN
                    @prev_id != @cur_id
                        AND t1.location_id IS NULL
                THEN
                    @location_id:=t1.location_id
                WHEN
                    @prev_id = @cur_id
                        AND t1.location_id IS NOT NULL
                        AND @location_id IS NULL
                THEN
                    @location_id:=t1.location_id
                ELSE @location_id
            END AS last_location,
            CASE
                
				
				WHEN
                    @prev_id != @cur_id
                        AND t1.encounter_type IS NOT NULL
                THEN
                    @encounter_type:=t1.encounter_type
                WHEN
                    @prev_id != @cur_id
                        AND t1.encounter_type IS NULL
                THEN
                    @encounter_type:=t1.encounter_type
                WHEN
                    @prev_id = @cur_id
                        AND (t1.encounter_type IS NOT NULL or @encounter_type=99999)
                        AND @encounter_type IS NULL
                THEN
                    @encounter_type:=t1.encounter_type
                ELSE @encounter_type
            END AS last_encounter_type,
          arv_first_regimen_start_date,enrollment_date
    FROM
        (SELECT DISTINCT
        person_id,
            encounter_id,
            enrollment_date,
            arv_first_regimen_start_date,
            vl_1,
            vl_1_date,
            vl_2,
            vl_2_date,
            cur_arv_adherence,
            encounter_datetime,
            cd4_1,
            cd4_1_date,
            cur_arv_meds,
            cur_who_stage,
            location_id,
            encounter_type
        from etl.flat_hiv_summary_v15b
    WHERE

        arv_first_regimen_start_date  >= @startDate  and arv_first_regimen_start_date<= @endDate


        ORDER BY person_id , encounter_datetime DESC) t1) te 
    ;   
replace into ndwr.patint_current_data_2
SELECT DISTINCT
        person_id,
            last_vl_1,
            last_vl_1_date,
            vl_2,
            vl_2_date,
            cd4_1_latest,
            cd4_1_date_latest,
            enrollment_date,
            arv_first_regimen_start_date,
            cur_arv_adherence,
            cur_arv_meds,
            cur_who_stage,
            last_location ,
            last_encounter_type
    FROM 
        (SELECT
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
            
            CASE
                WHEN @prev_id != @cur_id AND t1.vl_1 THEN @vl_1:=t1.vl_1
                WHEN
                    @prev_id != @cur_id
                        AND (t1.vl_1 IS NULL OR t1.vl_1 = 0)
                THEN
                    @vl_1:=t1.vl_1
                WHEN
                    @prev_id = @cur_id AND t1.vl_1
                        AND @vl_1 IS NULL
                THEN
                    @vl_1:=t1.vl_1
                ELSE @vl_1
            END AS last_vl_1,
            CASE
                WHEN @prev_id != @cur_id AND t1.vl_1_date THEN @vl_1_date:=t1.vl_1_date
                WHEN
                    @prev_id != @cur_id
                        AND t1.vl_1_date IS NULL
                THEN
                    @vl_1_date:=t1.vl_1_date
                WHEN
                    @prev_id = @cur_id AND t1.vl_1_date
                        AND @vl_1_date IS NULL
                THEN
                    @vl_1_date:=t1.vl_1_date
                ELSE @vl_1_date
            END AS last_vl_1_date,  
            CASE
                WHEN @prev_id != @cur_id AND t1.vl_2 THEN @vl_2:=t1.vl_2
                WHEN
                    @prev_id != @cur_id
                        AND (t1.vl_2 IS NULL OR t1.vl_2 = 0)
                THEN
                    @vl_2:=t1.vl_2
                WHEN
                    @prev_id = @cur_id AND t1.vl_2
                        AND @vl_2 IS NULL
                THEN
                    @vl_2:=t1.vl_2
                ELSE @vl_2
            END AS vl_2,
                        CASE
                WHEN @prev_id != @cur_id AND t1.vl_2_date THEN @vl_2_date:=t1.vl_2_date
                WHEN
                    @prev_id != @cur_id
                        AND t1.vl_2_date IS NULL
                THEN
                    @vl_2_date:=t1.vl_2_date
                WHEN
                    @prev_id = @cur_id AND t1.vl_2_date
                        AND @vl_2_date IS NULL
                THEN
                    @vl_2_date:=t1.vl_2_date
                ELSE @vl_2_date
            END AS vl_2_date,
                        CASE
                WHEN @prev_id != @cur_id AND t1.cd4_1 THEN @cd4_1:=t1.cd4_1
                WHEN
                    @prev_id != @cur_id
                        AND (t1.cd4_1 IS NULL OR t1.cd4_1 = 0)
                THEN
                    @cd4_1:=t1.cd4_1
                WHEN
                    @prev_id = @cur_id AND t1.cd4_1
                        AND @cd4_1 IS NULL
                THEN
                    @cd4_1:=t1.cd4_1
                ELSE @cd4_1
            END AS cd4_1_latest,         
            CASE
                WHEN @prev_id != @cur_id AND t1.cd4_1_date THEN @cd4_1_date:=t1.cd4_1_date
                WHEN
                    @prev_id != @cur_id
                        AND (t1.cd4_1_date IS NULL OR t1.cd4_1_date = 0)
                THEN
                    @cd4_1_date:=t1.cd4_1_date
                WHEN
                    @prev_id = @cur_id AND t1.cd4_1_date
                        AND @cd4_1_date IS NULL
                THEN
                    @cd4_1_date:=t1.cd4_1_date
                ELSE @cd4_1_date
            END AS cd4_1_date_latest,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_adherence IS NOT NULL
                THEN
                    @cur_arv_adherence:=t1.cur_arv_adherence
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_adherence IS NULL
                THEN
                    @cur_arv_adherence:=t1.cur_arv_adherence
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_arv_adherence IS NOT NULL
                        AND @cur_arv_adherence IS NULL
                THEN
                    @cur_arv_adherence:=t1.cur_arv_adherence
                ELSE @cur_arv_adherence
            END AS cur_arv_adherence,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_meds IS NOT NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_meds IS NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_arv_meds IS NOT NULL
                        AND @cur_arv_meds IS NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                ELSE @cur_arv_meds
            END AS cur_arv_meds,
                      CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_who_stage IS NOT NULL
                THEN
                    @cur_who_stage:=t1.cur_who_stage
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_who_stage IS NULL
                THEN
                    @cur_who_stage:=t1.cur_who_stage
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_who_stage IS NOT NULL
                        AND @cur_who_stage IS NULL
                THEN
                    @cur_who_stage:=t1.cur_who_stage
                ELSE @cur_who_stage
            END AS cur_who_stage,
 CASE
                
				
				WHEN
                    @prev_id != @cur_id
                        AND t1.location_id IS NOT NULL
                THEN
                    @location_id:=t1.location_id
                WHEN
                    @prev_id != @cur_id
                        AND t1.location_id IS NULL
                THEN
                    @location_id:=t1.location_id
                WHEN
                    @prev_id = @cur_id
                        AND t1.location_id IS NOT NULL
                        AND @location_id IS NULL
                THEN
                    @location_id:=t1.location_id
                ELSE @location_id
            END AS last_location,
            CASE
                
				
				WHEN
                    @prev_id != @cur_id
                        AND t1.encounter_type IS NOT NULL
                THEN
                    @encounter_type:=t1.encounter_type
                WHEN
                    @prev_id != @cur_id
                        AND t1.encounter_type IS NULL
                THEN
                    @encounter_type:=t1.encounter_type
                WHEN
                    @prev_id = @cur_id
                        AND (t1.encounter_type IS NOT NULL or @encounter_type=99999)
                        AND @encounter_type IS NULL
                THEN
                    @encounter_type:=t1.encounter_type
                ELSE @encounter_type
            END AS last_encounter_type,
          arv_first_regimen_start_date,enrollment_date
    FROM
        (SELECT DISTINCT
        person_id,
            encounter_id,
            enrollment_date,
            arv_first_regimen_start_date,
            vl_1,
            vl_1_date,
            vl_2,
            vl_2_date,
            cur_arv_adherence,
            encounter_datetime,
            cd4_1,
            cd4_1_date,
            cur_arv_meds,
            cur_who_stage,
            location_id,
            encounter_type
        from etl.flat_hiv_summary_v15b
    WHERE        rtc_date >= @startDate 

        ORDER BY person_id , encounter_datetime DESC) t1) te 
    ;
  

insert into ndwr.patint_initial_data_2
SELECT DISTINCT
        person_id,
            enrollment_cd4_1,
            enrollment_cd4_1_date,
            first_arv_meds,
            arv_start_location
    FROM
        (SELECT
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
            CASE
                WHEN @prev_id != @cur_id AND t1.cd4_1 THEN @cd4_1:=t1.cd4_1
                WHEN
                    @prev_id != @cur_id
                        AND (t1.cd4_1 IS NULL OR t1.cd4_1 = 0)
                THEN
                    @cd4_1:=t1.cd4_1
                WHEN
                    @prev_id = @cur_id AND t1.cd4_1
                        AND @cd4_1 IS NULL
                THEN
                    @cd4_1:=t1.cd4_1
                ELSE @cd4_1
            END AS enrollment_cd4_1,         
            CASE
                WHEN @prev_id != @cur_id AND t1.cd4_1_date THEN @cd4_1_date:=t1.cd4_1_date
                WHEN
                    @prev_id != @cur_id
                        AND (t1.cd4_1_date IS NULL )
                THEN
                    @cd4_1_date:=t1.cd4_1_date
                WHEN
                    @prev_id = @cur_id AND t1.cd4_1_date
                        AND @cd4_1_date IS NULL
                THEN
                    @cd4_1_date:=t1.cd4_1_date
                ELSE @cd4_1_date
            END AS enrollment_cd4_1_date,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_meds IS NOT NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                WHEN
                    @prev_id != @cur_id
                        AND t1.cur_arv_meds IS NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                WHEN
                    @prev_id = @cur_id
                        AND t1.cur_arv_meds IS NOT NULL
                        AND @cur_arv_meds IS NULL
                THEN
                    @cur_arv_meds:=t1.cur_arv_meds
                ELSE @cur_arv_meds
            END AS first_arv_meds,
              CASE
                WHEN
                    arv_first_regimen_start_date='1900-01-01'
                        
                THEN  @location_id:=NULL
				
				WHEN
                    @prev_id != @cur_id
                        AND t1.location_id IS NOT NULL
                THEN
                    @location_id:=t1.location_id
                WHEN
                    @prev_id != @cur_id
                        AND t1.location_id IS NULL
                THEN
                    @location_id:=t1.location_id
                WHEN
                    @prev_id = @cur_id
                        AND t1.location_id IS NOT NULL
                        AND @location_id IS NULL
                THEN
                    @location_id:=t1.location_id
                ELSE @location_id
            END AS arv_start_location,

            arv_first_regimen_start_date
    FROM
        (SELECT DISTINCT
        person_id,
            arv_first_regimen_start_date,
            cur_arv_meds,
            cd4_1,
            cd4_1_date,
location_id
    FROM
        etl.flat_hiv_summary_v15b
        WHERE       encounter_datetime between  @startDate and @endDate
    ORDER BY person_id, encounter_datetime ) t1     order by person_id) te
    ;
    
insert into ndwr.patint_second_visit_data_2
SELECT DISTINCT
            person_id,
            tx_second_visit_status,
            tx_second_visit_date_time
    FROM
        (SELECT
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
            CASE
                WHEN @prev_id != @cur_id AND t1.scheduled_visit THEN @scheduled_visit:=t1.scheduled_visit
                WHEN
                    @prev_id != @cur_id  AND (t1.scheduled_visit IS NULL )
                THEN
                    @scheduled_visit:=t1.scheduled_visit
                WHEN
                    @prev_id = @cur_id AND t1.scheduled_visit AND @scheduled_visit IS NULL
                THEN
                    @scheduled_visit:=t1.scheduled_visit
                ELSE @scheduled_visit
            END AS tx_second_visit_status,         
            CASE
                WHEN @prev_id != @cur_id AND t1.encounter_datetime THEN @encounter_datetime:=t1.encounter_datetime
                WHEN
                    @prev_id != @cur_id
                        AND (t1.encounter_datetime IS NULL )
                THEN
                    @encounter_datetime:=t1.encounter_datetime
                WHEN
                    @prev_id = @cur_id AND t1.encounter_datetime
                        AND @encounter_datetime IS NULL
                THEN
                    @encounter_datetime:=t1.encounter_datetime
                ELSE @encounter_datetime
            END AS tx_second_visit_date_time
    FROM
        (SELECT 
    encounter_datetime,scheduled_visit,person_id
FROM
    etl.flat_hiv_summary_v15b
WHERE
     /*location_id=@location and */ #arv_first_regimen_start_date between  @startDate and @endDate  
     encounter_datetime between  @startDate and @endDate and 
      is_clinical_encounter=1 and  date(encounter_datetime)  > date(arv_first_regimen_start_date)
    
      ORDER BY person_id,encounter_datetime ) t1 )s
    ;
    
    
insert into ndwr.patient_last_followup_data_2  SELECT DISTINCT
            person_id,
            last_out_reach_visit_status,
            last_out_reach_visit_date
    FROM
        (SELECT
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
            CASE
                WHEN @prev_id != @cur_id AND t1.patient_care_status THEN @patient_care_status:=t1.patient_care_status
                WHEN
                    @prev_id != @cur_id  AND (t1.patient_care_status IS NULL )
                THEN
                    @patient_care_status:=t1.patient_care_status
                WHEN
                    @prev_id = @cur_id AND t1.patient_care_status AND @patient_care_status IS NULL
                THEN
                    @patient_care_status:=t1.patient_care_status
                ELSE @patient_care_status
            END AS last_out_reach_visit_status,         
            CASE
                WHEN @prev_id != @cur_id AND t1.encounter_datetime THEN @encounter_datetime:=t1.encounter_datetime
                WHEN
                    @prev_id != @cur_id
                        AND (t1.encounter_datetime IS NULL )
                THEN
                    @encounter_datetime:=t1.encounter_datetime
                WHEN
                    @prev_id = @cur_id AND t1.encounter_datetime
                        AND @encounter_datetime IS NULL
                THEN
                    @encounter_datetime:=t1.encounter_datetime
                ELSE @encounter_datetime
            END AS last_out_reach_visit_date
    FROM
        (SELECT 
    encounter_datetime,patient_care_status,person_id
FROM
    etl.flat_hiv_summary_v15b
WHERE
     encounter_datetime between  @startDate and @endDate and 
encounter_type=21 
    
ORDER BY person_id,encounter_datetime desc) t1 )s
    ; 
 



    insert into ndwr.ltfu_details_2
SELECT DISTINCT
            person_id,
            last_encounter_datetime,
            last_rtc_date,
             DATE_ADD(last_rtc_date, INTERVAL 28 DAY) after_28,
             #if(@endDate >(DATE_ADD(last_rtc_date, INTERVAL 28 DAY)),'ltfu','active')  as status, 
             last_death_date,
             if(last_death_date<last_encounter_datetime and last_death_date is not null ,'Wrong','') as death_status,
             CASE  
					WHEN patient_care_status=159 then 'DECEASED'
					WHEN last_death_date is not null then 'DECEASED'
					WHEN patient_care_status=9887 then 'Transfer Out'
					/*WHEN patient_care_status=1285 then 'Transfer Out'
					WHEN patient_care_status=9504 then 'Transfer Out'
					WHEN patient_care_status=1594 then 'Transfer Out'
					WHEN patient_care_status=9083 then 'Transfer Out'*/
					WHEN @endDate >(DATE_ADD(last_rtc_date, INTERVAL 28 DAY))  then 'LTFU'	
                    ELSE 'Active'
			END as care_status
    FROM
        (SELECT
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
          CASE
                WHEN @prev_id != @cur_id AND t1.rtc_date THEN @rtc_date:=t1.rtc_date
                WHEN
                    @prev_id != @cur_id
                        AND (t1.rtc_date IS NULL )
                THEN
                    @rtc_date:=t1.rtc_date
                WHEN
                    @prev_id = @cur_id AND t1.rtc_date
                        AND @rtc_date IS NULL
                THEN
                    @rtc_date:=t1.rtc_date
                ELSE @rtc_date
            END AS last_rtc_date,         
            CASE
                WHEN @prev_id != @cur_id AND t1.encounter_datetime THEN @encounter_datetime:=t1.encounter_datetime
                WHEN
                    @prev_id != @cur_id
                        AND (t1.encounter_datetime IS NULL )
                THEN
                    @encounter_datetime:=t1.encounter_datetime
                WHEN
                    @prev_id = @cur_id AND t1.encounter_datetime
                        AND @encounter_datetime IS NULL
                THEN
                    @encounter_datetime:=t1.encounter_datetime
                ELSE @encounter_datetime
            END AS last_encounter_datetime,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.patient_care_status IS NOT NULL
                THEN
                    @patient_care_status:=t1.patient_care_status
                WHEN
                    @prev_id != @cur_id
                        AND t1.patient_care_status IS NULL
                THEN
                    @patient_care_status:=t1.patient_care_status
                WHEN
                    @prev_id = @cur_id
                        AND t1.patient_care_status IS NOT NULL
                        AND @patient_care_status IS NULL
                THEN
                    @patient_care_status:=t1.patient_care_status
                ELSE @patient_care_status
            END AS patient_care_status ,
            
             CASE
                WHEN @prev_id != @cur_id AND t1.death_date THEN @death_date:=t1.death_date
                WHEN
                    @prev_id != @cur_id
                        AND (t1.death_date IS NULL )
                THEN
                    @death_date:=t1.death_date
                WHEN
                    @prev_id = @cur_id AND t1.death_date
                        AND @death_date IS NULL
                THEN
                    @death_date:=t1.death_date
                ELSE @death_date
            END AS last_death_date
    FROM
        (SELECT 
    encounter_datetime,rtc_date,person_id,transfer_out_date,patient_care_status,death_date
FROM
    etl.flat_hiv_summary_v15b
WHERE     encounter_datetime  >= @startDate  and encounter_datetime<= @endDate
    
ORDER BY person_id,encounter_datetime desc) t1 )
s
where s.last_rtc_date<>''

    ;
    
 
  END