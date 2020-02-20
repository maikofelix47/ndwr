CREATE  PROCEDURE `createPatientNDWRDataSets`(
  IN selectedMFLCode int(11),
  IN selectedFacility varchar(250),
  IN selectedPatient int(11)
  
  )
BEGIN
                 DECLARE selectedPeriod date; 
                 DECLARE selectedMFLCode INT Default 0;
                 Select reporting_period from ndwr.mfl_period limit 1 into selectedPeriod;
 		        Select mfl_code from ndwr.mfl_period limit 1 into selectedMFLCode;
 
                          
 						  set @selectedPatient:=selectedPatient;
                           Set @facilityName:= selectedFacility;
                           set @siteCode:= @selectedMFLCode; 
                           set @selectedPeriod:= selectedPeriod;
                           set @selectedMFLCode:= selectedMFLCode;
                              delete from  ndwr_base_line_0;
   							insert into ndwr_base_line_0						
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
                                   WHERE s.person_id =@selectedPatient
  								 and l.person_id =@selectedPatient
  								 and s.location_id in (
  								 select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  								 and s.person_id =@selectedPatient								 
  								 and (s.arv_first_regimen_start_date <> ''	or s.enrollment_date<>'')			
   							;
     
                          call buildNDWRPatientBaseline(@selectedMFLCode,selectedFacility,@selectedPatient);
  						
                          delete from ndwr.ndwr_visit_0;
  						
   						insert into ndwr.ndwr_visit_0 							
   							 SELECT
   										person_id,
   										cur_arv_adherence,
   										edd,
   										if(edd,'Yes',null) as pregnant,
   										if(edd,date_add(edd, interval -280 day),null) as LMP,
   										if(edd,datediff(encounter_datetime,date_add(edd, interval -280 day)),null) as gestation,
   										null as condoms_provided ,
   										contraceptive_method AS pwp,
   										IF(contraceptive_method,
   										contraceptive_method,
   										null) AS family_planning,
   										cur_who_stage,
   										encounter_datetime,
   										location_id,
   										scheduled_visit,
   										encounter_id,
   										rtc_date
   									FROM etl.flat_hiv_summary_v15b
  									WHERE person_id =@selectedPatient
  								    and location_id in (
  								         select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  								 
   							;						
   							
   							delete from ndwr.ndwroi;
   							insert into ndwr.ndwroi
   								select t1.person_id,
   								if(obs regexp "!!6042=" ,
   										replace(replace((substring_index(substring(obs,locate("!!6042=",obs)),'##',ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!6042=", "") ) ) / LENGTH("!!6042=") ))),"!!6042=",""),"!!",""),
   										null
   								) as OI, t1.encounter_datetime as OIDate 
   								
   								 from etl.flat_obs t1 
                                   inner join ndwr.ndwr_visit_0 t2 on t2.encounter_datetime=t1.encounter_datetime and t1.person_id=t2.person_id
   								where t1.person_id =@selectedPatient
  								and t1.location_id in (select location_id from ndwr.mfl_codes 
  								where mfl_code=@selectedMFLCode)
  								and obs regexp "!!6042="
   
   							;
                               
   							replace into ndwr.ndwr_vitals(
                              select distinct t1.person_id , weight as Weight, height as Height,
   							concat(systolic_bp,'/',diastolic_bp)AS BP, t1.encounter_datetime from  etl.flat_vitals t1
                               inner join  ndwr.ndwr_visit_0 t2  on t2.encounter_datetime=t1.encounter_datetime
                             where t1.person_id=@selectedPatient
  						   and t1.location_id in (select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)						   
                               );                                
   							
   							#Find lab_art_start_comparison					
   							#Pharmacy
                               
                               replace into ndwr.ndwr_patient_pharmacy(
                               SELECT  distinct			
                               person_id as PatientID,
                               person_id as PatientPK,
                               @facilityName AS FacilityName,
                               @siteCode AS SiteCode,
                               encounter_id as VisitID,
                               etl.get_arv_names(cur_arv_meds) as Drug,
                               encounter_datetime as DispenseDate,
                               'HIV Treatment' as TreatmentType,
                               null as ProphylaxisType,
                               rtc_date as ExpectedReturn,
                               DATEDIFF(rtc_date,encounter_datetime) as Duration,
                               DATEDIFF(rtc_date,encounter_datetime) as PeriodTaken,
                               'AMRS' as Emr,
                               'Ampath Plus' as Project,
                               null as DateImported,
                               null as Ident,
                               @siteCode as FacilityID
   							FROM
   								etl.flat_hiv_summary_v15b t1
   							WHERE 	t1.person_id =@selectedPatient
  							and t1.location_id 
  							in (select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  							and	t1.cur_arv_meds is not null);
  								
  							set @status=null;                            
  							set @last_encounter_date=null;                            
  							set @rtc_date=null; 
  							
                               insert into ndwr_all_patients (
                               SELECT 
                                 distinct                                   
                                   t1.person_id as PatientID,
                               t1.person_id as PatientPK,
                               @siteCode as SiteCode,
                               @facilityName as FacilityName,
                               gender AS Gender,
                               birthdate AS DOB,
                               t1.enrollment_date as RegistrationDate,
                               t1.enrollment_date as RegistrationAtCCC,
                               null as RegistrationAtPMTCT,
                               null as RegistrationAtTBClinic,
                               null as PatientSource,
                               clinic_county as Region,
                               null as District,
                               null as Village,
                               null as ContactRelation,
  							 case
   								when  @last_encounter_date is null then @last_encounter_date := t1.encounter_date
   								else @last_encounter_date
   							 end as LastVisit, 
                               null as MaritalStatus,
                               null as EducationLevel,
                               t1.enrollment_date as DateConfirmedHIVPositive,
                               null as PreviousARTExposure,
                               null as PreviousARTStartDate,
                               'AMRS' as Emr,
                               'Ampath Plus' as Project,
                               @siteCode as FacilityID,
  							 case
   								when  @status is null then @status := t1.status
   								else @status
   							 end as StatusAtCCC, 
                               null as StatusAtPMTCT,
                               null as StatusAtTBClinic,
                               null as SatelliteName,
  							 if(t1.arv_first_regimen_start_date,t1.arv_first_regimen_start_date,t1.enrollment_date) as arv_first_regimen_start_date,
  							 
  							case
   								when  @rtc_date is null then @rtc_date := t1.rtc_date
   								else @rtc_date
   							 end as rtc_date, 							 
  							 if(t1.arv_first_regimen,etl.get_arv_names(t1.arv_first_regimen),'unknown') as arv_first_regimen,
  							 if(t1.arv_first_regimen_start_date,t1.arv_first_regimen_start_date,t1.enrollment_date) as arv_start_date,
  							 etl.get_arv_names(t1.cur_arv_meds) as cur_arv_meds,
  							 t1.cur_arv_line_strict
                               FROM etl.hiv_monthly_report_dataset_frozen t1 
                               
  							 WHERE 	t1.person_id =@selectedPatient
 								and enddate=@selectedPeriod
  								and t1.location_id 
  								in (select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  								order by t1.encounter_date desc
                               );
   
                     
                      
                       insert into ndwr_all_patient_visits(PatientID,PatientPK,FacilityID,FacilityName,SiteCode,VisitID,VisitDate,SERVICE,VisitType,WHOStage,WABStage,Pregnant,LMP,EDD,Height,BP,OI,OIDate,Adherence,AdherenceCategory,FamilyPlanningMethod,PwP,GestationAge,NextAppointmentDate,SubstitutionFirstlineReg,Emr,Project,DateImported,Ident,SecondlineRegimenChangeDate,SubstitutionSecondlineRegimenDate,SubstitutionFirstlineRegimenDate,Weight) (
                       SELECT distinct
                           e.person_id AS PatientID,
                           e.person_id AS PatientPK,
   						@siteCode  as FacilityID,
                           @facilityName AS FacilityName,
   					    @siteCode AS SiteCode,
                           e.encounter_id AS VisitID,
                           e.encounter_datetime AS VisitDate,
                           'HIV Care' as SERVICE,
                           if(cn.name is not null,cn.name,'Unknownknown') as Visittype,
                           e.cur_who_stage AS WHOStage,
                           null  AS WABStage,
                           e.pregnant AS Pregnant,
                           e.LMP AS LMP,
                           e.edd as EDD,
                           v.Height AS Height,                        
                           v.bp AS BP,
                           o.OI AS OI,
                           o.OIDate AS OIDate,
                           e.cur_arv_adherence AS Adherence,
                           e.cur_arv_adherence AS AdherenceCategory,
                           e.family_planning AS FamilyPlanningMethod,
                           e.pwp AS PwP,
                           e.gestation AS GestationAge,
                           e.rtc_date as NextAppointmentDate,
                           null as SubstitutionFirstlineReg,
                           'AMRS' AS Emr,
                           'Ampath Plus' AS Project,
                           null AS DateImported,
                           null AS Ident,
                           null as SecondlineRegimenChangeDate,
                           null as SubstitutionSecondlineRegimenDate,
                           null as SubstitutionFirstlineRegimenDate,
                           v.Weight AS Weight
                           FROM      ndwr.ndwr_visit_0 e left join ndwr_vitals v on v.person_id=e.person_id and v.encounter_datetime=e.encounter_datetime
                           left join ndwroi  o  on o.person_id=e.person_id and o.OIDate=e.encounter_datetime
						   left join amrs.concept_name cn on cn.concept_id=e.scheduled_visit and cn.concept_name_type='FULLY_SPECIFIED' and voided<>1
                       ); 
             
               insert into ndwr.ndwr_patient_status(
  			 PatientID,
  			 PatientPK,FacilityName,SiteCode,ExitDescription,ExitReason,ExitDate,Emr,Project,Ident,DateImported,FacilityId)(
                select
                   t1.PatientID,
                   t1.PatientPK,
                   t1.FacilityName,
   				 t1.SiteCode,
  				 if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitDescription,
                   if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitReason,
                   if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),t1.lastVisit,null)ExitDate,
                   t1.Emr,
                   t1.Project,
                   null as Ident,
  				 null as DateImported,t1.FacilityId
                from ndwr.ndwr_all_patients t1 where t1.StatusAtCCC in('dead','ltfu','transfer_out')
  			  and t1.PatientID=@selectedPatient 
               );
   
               # ART ndwr_patient_status
  			 set @cur_arv_meds=null;
  			 set @cur_arv_line_strict=null;
  			 insert into ndwr_art_patients           
               select  distinct
               t1.lastVisit as encounter_date,
               t1.PatientID,
               t1.PatientPK,
               t1.DOB as DOB,
               t1.arv_first_regimen_start_date as arv_first_regimen_start_date,
               DATEDIFF(t1.RegistrationDate,DOB)/365.25 as AgeEnrollment,    if(sign(DATEDIFF(t1.arv_first_regimen_start_date,DOB)/365.25)=-1,DATEDIFF(t1.RegistrationDate,DOB)/365.25,
  			 DATEDIFF(t1.arv_first_regimen_start_date,t1.DOB)/365.25) as AgeARTStart,
                   DATEDIFF(t1.lastVisit,t1.DOB)/365.25 as AgeLastVisit,
               t1.SiteCode,
               t1.FacilityName,
               t1.RegistrationDate,
               null as PatientSource,
               t1.gender as Gender,
               t1.arv_first_regimen_start_date as StartARTDate,
               t1.arv_first_regimen_start_date as PreviousARTStartDate,
               etl.get_arv_names(t1.arv_first_regimen) as PreviousARTRegimen,
               t1.arv_start_date as StartARTAtThisFacility,
                t1.arv_first_regimen as StartRegimen,
  			 case
   								when  @cur_arv_line_strict is null 
  								then @cur_arv_line_strict := t1.cur_arv_line_strict
   								else @cur_arv_line_strict 
   			 end as StartRegimenLine,
               t1.lastVisit as LastARTDate,			 
  			 case
   								when  @cur_arv_meds is null then @cur_arv_meds := t1.cur_arv_meds
   								else @cur_arv_meds 
   			 end as LastRegimen,
  			 case
   								when  @cur_arv_line_strict is null then @cur_arv_line_strict := t1.cur_arv_line_strict
   								else @cur_arv_line_strict
   			 end as LastRegimenLine,
               DATEDIFF(t1.rtc_date,t1.lastVisit) as Duration,
               t1.rtc_date as ExpectedReturn,
               1 as Provider,
               t1.LastVisit,
               1 as encounter_type,
               t1.StatusAtCCC as status,
               if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitReason,
               if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),t1.lastVisit,null)ExitDate,
               t1.Emr,
               t1.Project,
               null as Ident,
               null as PreviousARTRegimen_Orig,
               null as StartRegimen_Orig,
               null as LastRegimen_Orig,
               null as DateImported,
               t1.FacilityID
               FROM ndwr.ndwr_all_patients t1 where patientid=@selectedPatient;
  			 
             insert into ndwr.ndwr_patient_labs(PatientID,PatientPK,FacilityID,FacilityName,SiteCode,SatelliteName,OrderedbyDate,ReportedbyDate,VisitID,TestName,TestResult,BaselineTest,EnrollmentTest,Emr,Project,Ident,DateImported)
                   (SELECT 
                       t.person_id AS PatientID,
                       t.person_id AS PatientPK,
                       @siteCode AS FacilityID,
                       @facilityName AS FacilityName,
   					@siteCode AS SiteCode,
                       null AS SatelliteName,
                       t.test_datetime as OrderedbyDate,
                       t.test_datetime as ReportedbyDate,
                       t.VisitID,
                       t.TestName,
                       t.TestResult,
                       null AS BaselineTest,
                       null AS EnrollmentTest,
                       'AMRS' AS Emr,
                       'Ampath Plus' AS Project,
                       null AS Ident,
                       null AS DateImported
                           
                   FROM
                       (SELECT 
                           t1.person_id,
                               t1.test_datetime,
                               'CD4 Count' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t1.obs,
                                                                           LOCATE('!!5497=', t1.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!5497=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED) AS TestResult,
                               encounter_id as VisitID
   
                       FROM
                       etl.flat_lab_obs t1       
  					 WHERE 	t1.person_id = @selectedPatient					 
  					 and t1.obs REGEXP '!!5497=[0-9]' 
                           
                           
                           UNION  
                           
                           SELECT 
                           t2.person_id,
                               t2.test_datetime,
                               'CD4 %' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t2.obs,
                                                                           LOCATE('!!730=', t2.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!730=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED)   AS TestResult,
                               t2.encounter_id as VisitID
   
                       FROM
                           etl.flat_lab_obs t2   WHERE t2.person_id = @selectedPatient and t2.obs REGEXP '!!730=[0-9]'
                           
                           Union 
                           SELECT 
                           t3.person_id,
                           t3.test_datetime,
                               'VL' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t3.obs,
                                                                           LOCATE('!!856=', t3.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!856=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED) AS TestResult,
                               t3.encounter_id as VisitID
   
                       FROM
                           etl.flat_lab_obs  t3  WHERE   t3.person_id = @selectedPatient and t3.obs REGEXP '!!856=[0-9]'
                           
                           ) t);         
                                         
   END