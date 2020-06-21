DELIMITER $$
CREATE  PROCEDURE `buildNDWRPatientBaseline`(
IN selectedMFLCode INT,
IN selectedFacility varchar(200),
IN selectedPatient INT)
BEGIN
	
 		 Set @facilityName:= selectedFacility;
    set @siteCode := selectedMFLCode;
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
 		
 		insert into base_temp_1
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
 							
 							
              FROM ndwr_base_line_0 t1 where person_id=selectedPatient order by close_first_regimen_start desc;
 							
 							insert into patient_base_line(person_id,lastCd4,lastCd4_date,last_who_stage,last_who_stage_date,last_cd4_percent,last_cd4_percent_date,cd4_percent_at_arv_start,cd4_percent_at_arv_start_date,cd4_at_arv_start,cd4_at_arv_start_date,who_stage_at_arv_start,who_stage_at_arv_start_date,cd4_before_arv_start,cd4_date_before_arv_start,cur_who_stage_before_arv_start,cur_who_stage_date_before_arv_start,cd4_percent_before_arv_start,cd4_percent_date_before_arv_start,cd4_after_arv_start,cd4_date_after_arv_start,cd4_percent_after_arv_start,cd4_percent_date_after_arv_start,cur_who_stage_after_6month_arv,cur_who_stage_date_after_6month_arv,v_l_after_6month_arv,v_l_date_after_6month_arv,cd4_percent_after_6month_arv,cd4_percent_date_after_6month_arv,cd4_after_6month_arv,cd4_date_after_6month_arv,cur_who_stage_after_12month_arv,cur_who_stage_date_after_12month_arv,cd4_percent_after_12month_arv,cd4_percent_date_after_12month_arv,cd4_after_12month_arv,cd4_date_after_12month_arv,v_l_after_12month_arv,v_l_date_after_12month_arv) values(selectedPatient,@lastCd4,@lastCd4_date,
 							@last_who_stage,@last_who_stage_date,@last_cd4_percent,@last_cd4_percent_date,@cd4_percent_at_arv_start,@cd4_percent_at_arv_start_date,@cd4_at_arv_start,@cd4_at_arv_start_date,@who_stage_at_arv_start,@who_stage_at_arv_start_date,@cd4_before_arv_start,@cd4_date_before_arv_start,@cur_who_stage_before_arv_start,@cur_who_stage_date_before_arv_start,@cd4_percent_before_arv_start,@cd4_percent_date_before_arv_start,@cd4_after_arv_start,@cd4_date_after_arv_start,@cd4_percent_after_arv_start,@cd4_percent_date_after_arv_start,@cur_who_stage_after_6month_arv,@cur_who_stage_date_after_6month_arv,@v_l_after_6month_arv,@v_l_date_after_6month_arv,@cd4_percent_after_6month_arv,@cd4_percent_date_after_6month_arv,@cd4_after_6month_arv,@cd4_date_after_6month_arv,@cur_who_stage_after_12month_arv,@cur_who_stage_date_after_12month_arv,@cd4_percent_after_12month_arv,@cd4_percent_date_after_12month_arv,@cd4_after_12month_arv,@cd4_date_after_12month_arv,@v_l_after_12month_arv,
 							@v_l_date_after_12month_arv);					
 							
        replace into  ndwr.ndwr_base_line( 						
 							select 
                         person_id as PatientPK,
                         person_id as PatientID,
                         @facilityName AS FacilityName,
                         @siteCode AS SiteCode,
                         @siteCode as LocationID,
                         cd4_before_arv_start as bCD4,
                         cd4_date_before_arv_start as bCD4Date,
                         cur_who_stage_before_arv_start as bWHO,
                         cur_who_stage_date_before_arv_start as bWHODate,
                         cd4_at_arv_start as eCD4,
                         cd4_at_arv_start_date as eCD4Date,
                         who_stage_at_arv_start as eWHO,
                         who_stage_at_arv_start_date as eWHODate,
                         last_who_stage as lastWHO,
                         last_who_stage_date as lastWHODate,
                         lastCd4 as lastCD4,
                         lastCd4_date as lastCD4Date,
                         cd4_after_12month_arv as m12CD4,
                         cd4_date_after_12month_arv as m12CD4Date,
                         cd4_after_6month_arv as m6CD4,
                         cd4_date_after_6month_arv as m6CD4Date,
                         cd4_at_arv_start as CD4atEnrollment,
                         cd4_at_arv_start_date as CD4atEnrollment_Date,
                         cd4_before_arv_start as CD4BeforeARTStart,
                         cd4_date_before_arv_start as CD4BeforeARTStart_Date,
                         lastCd4 as LastCD4AfterARTStart,
                         lastCd4_date as LastCD4AfterARTStart_Date,
                         cd4_percent_at_arv_start as CD4atEnrollmentPercent,
                         cd4_percent_at_arv_start_date as CD4atEnrollmentPercent_Date,
                         cd4_percent_before_arv_start as CD4BeforeARTStartPercent,
                         cd4_percent_date_before_arv_start as CD4BeforeARTStartPercent_Date,
                         last_cd4_percent as LastCD4AfterARTStartPercent,
                         last_cd4_percent_date as LastCD4AfterARTStartPercent_Date,
                         cd4_after_6month_arv as 6MonthCD4,
                         cd4_date_after_6month_arv as 6MonthCD4_Date,
                         cd4_after_12month_arv as 12MonthCD4,
                         cd4_date_after_12month_arv as 12MonthCD4_Date,
                         cd4_percent_after_12month_arv as 6MonthCD4Percent,
                         cd4_percent_date_after_12month_arv as 6MonthCD4Percent_Date,
                         cd4_after_arv_start as FirstCD4AfterARTStart,
                         cd4_date_after_arv_start as FirstCD4AfterARTStart_Date,
                         cd4_percent_after_arv_start as FirtsCD4AfterARTStartPercent,
                         cd4_percent_date_after_arv_start as FirtsCD4AfterARTStartPercent_date,
                         null as Imported,
                         v_l_after_6month_arv as 6MonthVL,
                         v_l_date_after_6month_arv as 6MonthVlDate,
                         v_l_after_12month_arv as 12MonthVL,
                         v_l_date_after_12month_arv as 12MonthVLDate,
                         if(lastCd4_date,lastCd4_date,v_l_date_after_12month_arv) as VisitDate,
                         @siteCode as FacilityID
                                 from patient_base_line where person_id=selectedPatient);
                                 
				#copy data to patient_baseline_extract
                insert into ndwr.ndwr_patient_baselines_extract (
                  select 
                     PatientPK,
                     PatientID,
                     SiteCode as FacilityId,
                     SiteCode,
                     'AMRS' as EMR,
                     'Ampath' as Project,
					 bCD4,
                     bCD4Date,
                     null as bWAB,
                     null as bWABDate,
                     bWHO,
                     bWHODate,
                     null as eWAB,
                     null as eWABDate,
					 eCD4,
					 eCD4Date,
                     eWHO,
				     eWHODate,
                     lastWHO,
					 lastWHODate,
                     lastCD4,
					 lastCD4Date,
                     null as lastWAB,
                     null as lastWABDate,
                     12MonthCD4 as m12CD4,
                     12MonthCD4_Date as m12CD4Date,
                     6MonthCD4 as m6CD4,
                     6MonthCD4_Date as m6CD4Date
				  from 
                   ndwr.ndwr_base_line
                   where PatientID=selectedPatient
                );
                                 
 END$$
DELIMITER ;
