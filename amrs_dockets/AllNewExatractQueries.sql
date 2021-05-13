#1.AllergiesChronicIllness
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
      ChronicIllness,
      ChronicOnsetDate,
      knownAllergies,
      AllergyCausativeAgent,
      AllergicReaction,
      AllergySeverity,
      AllergyOnsetDate,
      Skin,
      Eyes,
      ENT,
      Chest,
      CVS,
      Abdomen,
      CNS,
      Genitourinary
	FROM
	    ndwr.ndwr_patient_allergies_chronic_illness;

-------------------------------------------------------

#2.IPT
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
    CASE
      WHEN OnTBDrugs = 1 THEN 'YES'
      WHEN OnTBDrugs = 0 THEN 'NO'
      ELSE NULL
    END AS 'OnTBDrugs',
    CASE
      WHEN OnIPT = 1 THEN 'YES'
      WHEN OnIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'OnIPT',
    CASE
      WHEN EverOnIPT = 1 THEN 'YES'
      WHEN EverOnIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'EverOnIPT',
    CASE
      WHEN Cough = 1 THEN 'YES'
      WHEN Cough = 0 THEN 'NO'
      ELSE NULL
    END AS 'Cough',
    CASE
      WHEN Fever = 1 THEN 'YES'
      WHEN Fever = 0 THEN 'NO'
      ELSE NULL
    END AS 'Fever',
    CASE
      WHEN NoticeableWeightLoss = 1 THEN 'YES'
      WHEN NoticeableWeightLoss = 0 THEN 'NO'
      ELSE NULL
    END AS 'NoticeableWeightLoss',
	CASE
      WHEN NightSweats = 1 THEN 'YES'
      WHEN NightSweats = 0 THEN 'NO'
      ELSE NULL
    END AS 'NightSweats',
    CASE
      WHEN  Lethargy = 1 THEN 'YES'
      WHEN  Lethargy = 0 THEN 'NO'
      ELSE NULL
    END AS 'Lethargy',
    ICFActionTaken,
    TestResult,
    TBClinicalDiagnosis,
    ContactsInvited,
    CASE
      WHEN  EvaluatedForIPT = 1 THEN 'YES'
      WHEN  EvaluatedForIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'EvaluatedForIPT',
    CASE
      WHEN  StartAntiTBs = 1 THEN 'YES'
      WHEN  StartAntiTBs = 0 THEN 'NO'
      ELSE NULL
    END AS 'StartAntiTBs',
    TBRxStartDate,
    TBScreening,
    IPTClientWorkUp,
    CASE
      WHEN  StartIPT = 1 THEN 'YES'
      WHEN  StartIPT = 0 THEN 'NO'
      ELSE NULL
    END AS 'StartIPT',
    IndicationForIPT
FROM
    ndwr.ndwr_patient_ipt_extract;


---------------------------------------------------

  #3.DepressionSecreening

	SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    FacilityID,
    Emr,
    Project,
    VisitID,
    VisitDate,
    CASE
        WHEN PHQ9_1 = 0 THEN 'Not at all'
        WHEN PHQ9_1 = 1 THEN 'Several days'
        WHEN PHQ9_1 = 2 THEN 'More than half'
        WHEN PHQ9_1 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-1',
    CASE
        WHEN PHQ9_2 = 0 THEN 'Not at all'
        WHEN PHQ9_2 = 1 THEN 'Several days'
        WHEN PHQ9_2 = 2 THEN 'More than half'
        WHEN PHQ9_3 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-2',
    CASE
        WHEN PHQ9_3 = 0 THEN 'Not at all'
        WHEN PHQ9_3 = 1 THEN 'Several days'
        WHEN PHQ9_3 = 2 THEN 'More than half'
        WHEN PHQ9_3 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-3',
    CASE
        WHEN PHQ9_4 = 0 THEN 'Not at all'
        WHEN PHQ9_4 = 1 THEN 'Several days'
        WHEN PHQ9_4 = 2 THEN 'More than half'
        WHEN PHQ9_4 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-4',
    CASE
        WHEN PHQ9_5 = 0 THEN 'Not at all'
        WHEN PHQ9_5 = 1 THEN 'Several days'
        WHEN PHQ9_5 = 2 THEN 'More than half'
        WHEN PHQ9_5 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-5',
    CASE
        WHEN PHQ9_6 = 0 THEN 'Not at all'
        WHEN PHQ9_6 = 1 THEN 'Several days'
        WHEN PHQ9_6 = 2 THEN 'More than half'
        WHEN PHQ9_6 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-6',
    CASE
        WHEN PHQ9_7 = 0 THEN 'Not at all'
        WHEN PHQ9_7 = 1 THEN 'Several days'
        WHEN PHQ9_7 = 2 THEN 'More than half'
        WHEN PHQ9_7 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-7',
    CASE
        WHEN PHQ9_8 = 0 THEN 'Not at all'
        WHEN PHQ9_8 = 1 THEN 'Several days'
        WHEN PHQ9_8 = 2 THEN 'More than half'
        WHEN PHQ9_8 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-8',
    CASE
        WHEN PHQ9_9 = 0 THEN 'Not at all'
        WHEN PHQ9_9 = 1 THEN 'Several days'
        WHEN PHQ9_9 = 2 THEN 'More than half'
        WHEN PHQ9_9 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9-9',
    CASE
        WHEN PHQ9Score >= 0 AND PHQ9Score <= 4 THEN 'Depression unlikely'
        WHEN PHQ9Score >= 5 AND PHQ9Score <= 9 THEN 'Mild depression'
        WHEN PHQ9Score >= 10 AND PHQ9Score <= 14 THEN 'Moderate depression'
        WHEN PHQ9Score >= 15 AND PHQ9Score <= 19 THEN 'Moderate severe depression'
        WHEN PHQ9Score >= 20 AND PHQ9Score <= 27 THEN 'Severe depression'
        ELSE NULL
    END AS 'PHQ9Score',
    PHQ9Rating AS 'PHQ9 Rating'
FROM
    ndwr.ndwr_patient_depression_screening;


    --------------------------------------------------

    #4.ndwr_patient_contact_listing
SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    FacilityName,
    Emr,
    Project,
    PartnerPersonID,
    ContactAge,
    CASE
        WHEN ContactSex = 1 THEN 'Male'
        WHEN ContactSex = 2 THEN 'Female'
        ELSE NULL
    END AS 'ContactSex',
    CASE
        WHEN ContactMaritalStatus = 5555 THEN 'Married monogamous'
        WHEN ContactMaritalStatus = 6290 THEN 'Married polygamous'
        WHEN ContactMaritalStatus = 1058 THEN 'Divorced'
        WHEN ContactMaritalStatus = 1059 THEN 'Widowed'
        WHEN ContactMaritalStatus = 1057 THEN 'Single'
        WHEN ContactMaritalStatus = 1175 THEN 'N/A (Child)'
        ELSE NULL
    END AS 'ContactMaritalStatus',
    CASE
        WHEN RelationshipWithPatient = 970 THEN 'Mother'
        WHEN RelationshipWithPatient = 971 THEN 'Father'
        WHEN RelationshipWithPatient = 972 THEN 'Sibling'
        WHEN RelationshipWithPatient = 1565 THEN 'Child'
        WHEN RelationshipWithPatient = 1669 THEN 'Sexual partner-spouse'
        WHEN RelationshipWithPatient = 1670 THEN 'Sexual partner-other'
        WHEN RelationshipWithPatient = 7246 THEN 'Co-wife'
        WHEN RelationshipWithPatient = 105 THEN 'Injectable drug user'
        ELSE NULL
    END AS 'RelationshipWithPatient',
    CASE
        WHEN ScreenedForIpv = 1 THEN 'Yes'
        WHEN ScreenedForIpv = 0 THEN 'No'
        ELSE NULL
    END AS 'ScreenedForIpv',
    CASE
        WHEN IpvScreening = 9303 THEN 'Sexual'
        WHEN IpvScreening = 1789 THEN 'Physical'
        WHEN IpvScreening = 7020 THEN 'Emotional'
        WHEN IpvScreening = 1107 THEN 'No IPV'
        WHEN IpvScreening = 1175 THEN 'N/A (Child)'
        ELSE NULL
    END AS 'IpvScreening',
    CASE
        WHEN IpvScreeningOutcome = 9303 THEN 'Sexual'
        WHEN IpvScreeningOutcome = 1789 THEN 'Physical'
        WHEN IpvScreeningOutcome = 7020 THEN 'Emotional'
        WHEN IpvScreeningOutcome = 1107 THEN 'No IPV'
        WHEN IpvScreeningOutcome = 1175 THEN 'N/A (Child)'
        ELSE NULL
    END AS 'IpvScreeningOutcome',
    CASE
        WHEN CurrentlyLivingWithIndexClient = 1 THEN 'Yes'
        WHEN CurrentlyLivingWithIndexClient = 0 THEN 'No'
        ELSE NULL
    END AS 'CurrentlyLivingWithIndexClient',
    CASE
        WHEN KnowledgeOfHivStatus = 1 THEN 'Yes'
        WHEN KnowledgeOfHivStatus = 0 THEN 'No'
        ELSE NULL
    END AS 'KnowledgeOfHivStatus',
    CASE
        WHEN PnsApproach = 11734 THEN 'Dual referral'
        WHEN PnsApproach = 11733 THEN 'Provider referral'
        WHEN PnsApproach = 9025 THEN 'Contract referral'
        WHEN PnsApproach = 10648 THEN 'Passive referral'
        ELSE NULL
    END AS 'PnsApproach'
FROM
    ndwr.ndwr_patient_contact_listing;


    ----------------------------------------------

  #5.GBVScreening

  SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    PartnerPersonID,
    VisitID,
    VisitDate,
    CASE
      WHEN IPV = 1 THEN 'Yes'
      WHEN IPV = 0 THEN 'NO'
      ELSE NULL
    END AS 'IPV',
    CASE
      WHEN PhysicalIPV = 1 THEN 'Yes'
      WHEN PhysicalIPV = 0 THEN 'NO'
      ELSE NULL
    END AS 'PhysicalIPV',
    CASE
      WHEN EmotionalIPV = 1 THEN 'Yes'
      WHEN EmotionalIPV = 0 THEN 'NO'
      ELSE NULL
    END AS 'EmotionalIPV',
	CASE
      WHEN SexualIPV = 1 THEN 'Yes'
      WHEN SexualIPV = 0 THEN 'NO'
      ELSE NULL
    END AS 'SexualIPV',
    CASE
      WHEN IPVRelationship = 1 THEN 'Yes'
      WHEN IPVRelationship = 0 THEN 'NO'
      ELSE NULL
    END AS 'IPVRelationship',
    DateCreated
FROM
    ndwr.ndwr_gbv_screening

-----------------------------------------------------

  #6.Enhanced Adherence Counselling
SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    SessionNumber,
    FirstSessionDate,
    PillCountAdherence,
    CASE
     WHEN MMAS4_1 = 1 THEN 'YES'
     WHEN MMAS4_1 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4-1',
    CASE
     WHEN MMAS4_2 = 1 THEN 'YES'
     WHEN MMAS4_2 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4-2',
	CASE
     WHEN MMAS4_3 = 1 THEN 'YES'
     WHEN MMAS4_3 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4-3',
    CASE
     WHEN MMAS4_4 = 1 THEN 'YES'
     WHEN MMAS4_4 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4_4',
	CASE
     WHEN MMAS4_4 = 1 THEN 'YES'
     WHEN MMAS4_4 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMAS4_4',
    CASE
     WHEN MMSA8_1 = 1 THEN 'YES'
     WHEN MMSA8_1 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMSA8_1',
    CASE
     WHEN MMSA4_2 = 1 THEN 'YES'
     WHEN MMSA4_2 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMSA4_2',
    CASE
     WHEN MMSA4_3 = 1 THEN 'YES'
     WHEN MMSA4_3 = 0 THEN 'NO'
     ELSE NULL
    END AS 'MMSA4_3',
    CASE
     WHEN MMSA4_4 = 0 THEN 'Never/Rarely'
     WHEN MMSA4_4 = 0.25 THEN 'Once in a while'
     WHEN MMSA4_4 = 0.5 THEN 'Sometimes'
     WHEN MMSA4_4 = 0.75 THEN 'Usually'
     WHEN MMSA4_4 = 1 THEN 'All the time'
     ELSE NULL
    END AS 'MMSA4_4',
    CASE
     WHEN MMSAScore >= 0 AND MMSAScore <= 0.25 THEN 'Good'
     WHEN MMSAScore >= 0.5 AND MMSAScore <= 2 THEN 'Inadequate'
     WHEN MMSAScore >= 3 AND MMSAScore <= 8 THEN 'Poor'
     ELSE 'Unknown'
    END AS 'MMSAScore',
    EACRecievedVL,
    EACVL,
    EACVLConcerns,
    EACVLThoughts,
    EACWayForward,
    EACCognitiveBarrier,
    EACBehaviouralBarrier_1,
    EACBehaviouralBarrier_2,
    EACBehaviouralBarrier_3,
    EACBehaviouralBarrier_4,
    EACBehaviouralBarrier_5,
    EACEmotionalBarriers_1,
    EACEmotionalBarriers_2,
    EACEconBarrier_1,
    EACEconBarrier_2,
    EACEconBarrier_3,
    EACEconBarrier_4,
    EACEconBarrier_5,
    EACEconBarrier_6,
    EACEconBarrier_7,
    EACEconBarrier_8,
    EACReviewImprovement,
    EACReviewMissedDoses,
    EACReviewStrategy,
    EACReferral,
    EACReferralApp,
    EACReferralExperience,
    EACHomevisit,
    EACAdherencePlan,
    EACFollowupDate,
    DateCreated
FROM
    ndwr.ndwr_patient_eac

  --------------------------------------------------------------

   #7.DrugAndAlcoholScreening

    SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    CASE
        WHEN DrinkAlcohol = 1090  THEN 'Never'
        WHEN DrinkAlcohol = 1091  THEN 'Monthly or less'
        WHEN DrinkAlcohol = 1092  THEN '2 to 4 times a month'
        WHEN DrinkAlcohol = 1093  THEN '2 to 3 times a week'
        WHEN DrinkAlcohol IN (1094,1095)  THEN '4 or More Times a Week'
        ELSE NULL
    END AS 'DrinkAlcohol',
    NULL AS 'Smoking',
    NULL AS 'DrugUse'
FROM
    ndwr.ndwr_drug_alcohol_screening 


    -------------------------------------------------------------

    #8.OVC

    SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    OVCEnrollmentDate,
    RelationshiptoClient,
    EnrolledinCPIMS,
    CPIMSUniqueidentifier,
    PartnerOfferingOVCServices,
    OVCExitReason,
    ExitDate
    FROM
    ndwr.ndwr_ovc_patient_visits; 


  ---------------------------------------------------------------------

   #9.OTZ

    SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    Emr,
    Project,
    FacilityName,
    VisitID,
    VisitDate,
    OTZEnrollmentDate,
    TransferInStatus,
    Modulespreviouslycovered,
    Modulescompletedtoday,
    SupportGroupInvolvement,
    Remarks,
    TransitionattritionReason,
    OutcomeDate
    FROM
    ndwr.ndwr_otz_patient_visits;


---------------------------------------------------------------------

#10.Adverse Events
SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    FacilityID,
    EMR,
    Project,
    AdverseEvent,
    AdverseEventStartDate,
    AdverseEventEndDate,
    Severity,
    VisitDate,
    AdverseEventActionTaken,
    AdverseEventClinicalOutcome,
    AdverseEventIsPregnant,
    AdverseEventCause,
    AdverseEventRegimen
FROM
    ndwr.ndwr_patient_adverse_events;

  ---------------------------------------------------------------------

 #11.PatientStatusExatrct
		SELECT 
		    PatientPK,
            SiteCode,
		    PatientID,
		    FacilityID,
		    Emr,
		    Project,
		    FacilityName,
		    ExitDescription,
		    ExitDate,
		    ExitReason,
        TOVerified,
        TOVerifiedDate
		FROM
		    ndwr.ndwr_all_patient_status_extract;

------------------------------------------------------------------------

	#12.PatientARTExtract
	SELECT 
	    PatientPK,
      SiteCode,
	    PatientID,
	    FacilityID,
	    Emr,
	    Project,
	    FacilityName,
	    DOB,
	    AgeEnrollment,
	    AgeARTStart,
	    AgeLastVisit,
	    RegistrationDate,
	    PatientSource,
	    Gender,
	    StartARTDate,
	    PreviousARTStartDate,
	    PreviousARTRegimen,
	    StartARTAtThisFacility,
	    StartRegimen,
	    StartRegimenLine,
	    LastARTDate,
	    LastRegimen,
	    LastRegimenLine,
	    Duration,
	    ExpectedReturn,
	    Provider,
	    LastVisit,
	    ExitReason,
	    ExitDate
	FROM
	    ndwr.ndwr_patient_art_extract;


------------------------------------------------------------

	#13.AllPatients Extract
	SELECT 
      PKV,
	    PatientPK,
      SiteCode,
	    PatientID,
	    FacilityID,
	    Emr,
	    Project,
	    FacilityName,
	    Gender,
	    DOB,
	    RegistrationDate,
	    RegistrationAtCCC,
	    RegistrationAtPMTCT,
	    RegistrationAtTBClinic,
	    PatientSource,
	    Region,
	    District,
	    Village,
	    ContactRelation,
	    LastVisit,
	    MaritalStatus,
	    EducationLevel,
	    DateConfirmedHIVPositive,
	    PreviousARTExposure,
	    PreviousARTStartDate,
	    StatusAtCCC,
	    StatusAtPMTCT,
	    StatusAtTBClinic,
	    SatelliteName,
	    Inschool,
	    KeyPopulationType,
	    Orphan,
	    PatientResidentCounty,
	    PatientResidentLocation,
	    PatientResidentSubCounty,
	    PatientResidentSubLocation,
	    PatientResidentVillage,
	    PatientResidentWard,
	    PatientType,
	    PopulationType,
	    TransferInDate,
      Occupation
	FROM
	    ndwr.ndwr_all_patients_extract;

  --------------------------------------------------------------------------------------------
	
		#14.Patient Baseline Extract
		SELECT 
		    PatientPK,
        SiteCode,
		    PatientID,
		    FacilityID,
		    EMR,
		    Project,
		    bCD4,
		    bCD4Date,
		    bWAB,
		    bWABDate,
		    bWHO,
		    bWHODate,
		    eWAB,
		    eWABDate,
		    eCD4,
		    eCD4Date,
		    eWHO,
		    eWHODate,
		    lastWHO,
		    lastWHODate,
		    lastCD4,
		    lastCD4Date,
		    lastWAB,
		    lastWABDate,
		    m12CD4,
		    m12CD4Date,
		    m6CD4,
		    m6CD4Date
		FROM
		    ndwr.ndwr_patient_baselines_extract;

    ----------------------------------------------------------

    #15.PatientLabsExtract
	 SELECT 
    PatientPK,
    PatientID,
    FacilityID,
    SiteCode,
    Emr,
    Project,
    FacilityName,
    SatelliteName,
    VisitID,
    OrderedbyDate,
    ReportedbyDate,
    TestName,
    EnrollmentTest,
    TestResult,
    LabReason,
    DateSampleTaken,
    SampleType
FROM
    ndwr.ndwr_patient_labs_extract;


  ----------------------------------------------------

  	#16.PatientPharmacyextract
	SELECT 
	    PatientPK,
      SiteCode,
	    PatientID,
	    FacilityID,
	    Emr,
	    Project,
      FacilityName,
	    VisitID,
	    Drug,
	    Provider,
	    DispenseDate,
	    Duration,
	    ExpectedReturn,
	    TreatmentType,
	    RegimenLine,
	    PeriodTaken,
	    ProphylaxisType,
      RegimenChangedSwitched,
      RegimenChangeSwitchReason,
      StopRegimenReason,
      StopRegimenDate
	FROM
	    ndwr.ndwr_pharmacy;


  ----------------------------------------------------

  	#17.visits extract
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
	    Service,
	    VisitType,
      VisitBy,
      Height,
      Weight,
      Temp
      PulseRate
      RespiratoryRate,
      OxygenSaturation,
      MUAC,
      BP,
      NutritionStatus,
      PopulationType,
      KeyPopulationType,
	    WHOStage,
	    WABStage,
      EverHadMenses,
      LMP,
	    Pregnant,
	    Breastfeeding
	    EDD,
	    Menopausal
	    FamilyPlanningMethod,
	    NoFPReason,
	    OI,
	    OIDate,
      SystemExamination,
	    Adherence,
	    AdherenceCategory,
      CurrentRegimen,
      PwP,
      StabilityAssessment,
      HCWConcern,
      NextAppointmentDate,
      TCAReason,
      DifferentiatedCare,
	    SubstitutionFirstlineRegimenDate,
	    SubstitutionFirstlineRegimenReason,
	    SubstitutionSecondlineRegimenDate,
	    SubstitutionSecondlineRegimenReason,
	    SecondlineRegimenChangeDate,
	    SecondlineRegimenChangeReason,
	    ProphylaxisUsed,
	    CTXAdherence,
      ClinicalNotes
	FROM
	    ndwr.ndwr_all_patient_visits_extract;
 