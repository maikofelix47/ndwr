use ndwr;
SELECT 
    PatientPK,
    PatientID,
    FacilityID,
    SiteCode,
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
    ndwr.ndwr_patient_adverse_events
    join ndwr.ndwr_selected_site using (SiteCode)
    LIMIT 0;
	
========================================

SELECT 
    PatientPK,
    SiteCode,
    CASE
        WHEN PatientID IS NULL THEN PatientPK
        ELSE PatientID
    END AS 'PatientID',
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
    Temp,
    PulseRate,
    RespiratoryRate,
    OxygenSaturation,
    MUAC as 'Muac',
    BP,
    NutritionStatus as 'NutritionalStatus',
    'GeneralPopulation' AS 'PopulationType',
    NULL AS KeyPopulationType,
    WHOStage,
    WABStage,
    EverHadMenses,
    LMP,
    Pregnant,
    Breastfeeding,
    EDD,
    Menopausal, 
    FamilyPlanningMethod,
    NoFPReason,
    OI,
    OIDate,
    SystemExamination,
    Adherence,
    AdherenceCategory,
    CurrentRegimen,
    PwP,
    NULL AS 'GestationAge',
    StabilityAssessment,
    HCWConcern,
    IF(NextAppointmentDate,
        NextAppointmentDate,
        DATE_ADD(VisitDate, INTERVAL 21 DAY)) AS NextAppointmentDate,
    TCAReason,
    DifferentiatedCare,
    IF(SubstitutionFirstlineRegimenDate,
        SubstitutionFirstlineRegimenDate,
        NULL) AS SubstitutionFirstlineRegimenDate,
    SubstitutionFirstlineRegimenReason,
    IF(SubstitutionSecondlineRegimenDate,
        SubstitutionSecondlineRegimenDate,
        NULL) AS SubstitutionSecondlineRegimenDate,
    SubstitutionSecondlineRegimenReason,
    IF(SecondlineRegimenChangeDate,
        SecondlineRegimenChangeDate,
        NULL) AS SecondlineRegimenChangeDate,
    SecondlineRegimenChangeReason,
    ProphylaxisUsed,
    CTXAdherence,
    ClinicalNotes
FROM
    ndwr.ndwr_all_patient_visits_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
WHERE
    DATE(VisitDate) >= '1997-01-01';
		   
==========================================

SELECT 
    PatientPK,
    SiteCode,
    CASE
      WHEN PatientID IS NULL THEN 'PatientPK'
      ELSE PatientID
    END AS 'PatientID',
    FacilityID,
    Emr,
    Project,
    FacilityName,
    ExitDescription,
    ExitDate,
    ExitReason,
    NULL AS 'ReEnrollmentDate',
    TOVerified,
    TOVerifiedDate
FROM
    ndwr.ndwr_all_patient_status_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
group by PatientPK;
			
===============================================

SELECT 
    PatientPK,
    CASE
        WHEN PatientID = '' THEN PatientPK
        WHEN PatientID IS NULL THEN PatientPK
        ELSE PatientID
    END AS 'PatientID',
    FacilityID,
    SiteCode,
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
    ndwr.ndwr_patient_baselines_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode);
		
			
====================================================

SELECT 
    PatientPK,
    CASE
        WHEN PatientID IS NULL THEN PatientPK
        ELSE PatientID
    END AS 'PatientID',
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
    LabReason AS 'Reason',
    DateSampleTaken,
    SampleType
FROM
    ndwr.ndwr_patient_labs_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID;
		 
==================================================

SELECT 
    PatientPK,
    CASE
      WHEN PatientID IS NULL THEN PatientPK
      ELSE PatientID
    END AS 'PatientID',
    FacilityID,
    SiteCode,
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
    StartRegimenLineCategory AS `StartRegimenLine`,
    LastARTDate,
    LastRegimen,
    IF(LastRegimenLine IS NULL,
        1,
        LastRegimenLine) AS `LastRegimenLine`,
    Duration,
    IF(ExpectedReturn = NULL,
        DATE_ADD(LastVisit, INTERVAL 21 DAY),
        LastVisit) AS ExpectedReturn,
    Provider,
    LastVisit,
    ExitReason,
    ExitDate
FROM
    ndwr.ndwr_patient_art_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by PatientPK;
		
===============================================

SELECT 
    PatientPK,
    CASE
      WHEN PatientID IS NULL THEN PatientPK
      ELSE PatientID
    END AS 'PatientID',
    FacilityID,
    SiteCode,
    Emr,
    Project,
    VisitID,
    Drug,
    Provider,
    DispenseDate,
    Duration,
    IF(ExpectedReturn IS NULL
            || DATE(ExpectedReturn) = '0000-00-00',
        DATE_ADD(DispenseDate, INTERVAL 21 DAY),
        ExpectedReturn) AS ExpectedReturn,
    TreatmentType,
    RegimenLine,
    PeriodTaken,
    ProphylaxisType,
    RegimenChangedSwitched,
    RegimenChangeSwitchReason,
    StopRegimenReason,
    StopRegimenDate
FROM
    ndwr.ndwr_pharmacy
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
WHERE
    DATE(DispenseDate) >= '1997-01-01'
    group by VisitID;

===========================================

SELECT 
    PKV as 'Pkv',
    PatientPK,
    SiteCode,
    CASE
        WHEN PatientID IS NULL THEN PatientPK
        ELSE PatientID
    END AS 'PatientID',
    FacilityID,
    Emr,
    Project,
    FacilityName,
    NULL AS 'SatelliteName',
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
    CASE
        WHEN
            DateConfirmedHIVPositive IS NOT NULL
        THEN
            IF(DateConfirmedHIVPositive = '1900-01-01',
                '1997-01-01',
                DateConfirmedHIVPositive)
        ELSE DateConfirmedHIVPositive
    END AS `DateConfirmedHIVPositive`,
    PreviousARTExposure,
    PreviousARTStartDate,
    StatusAtCCC,
    StatusAtPMTCT,
    StatusAtTBClinic,
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
    ndwr.ndwr_all_patients_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
GROUP BY PatientPK;


===========================================


SELECT 
    PatientPK as 'patientPK',
    SiteCode as 'siteCode',
    PatientID as 'patientID',
	SiteCode as 'facilityId',
    FacilityName as 'facilityName',
    VisitID as 'visitID',
    VisitDate as 'visitDate',
    OTZEnrollmentDate as 'otzEnrollmentDate',
    TransferInStatus as 'transferInStatus',
    Modulespreviouslycovered as 'modulesPreviouslyCovered',
    Modulescompletedtoday AS 'modulesCompletedToday',
    SupportGroupInvolvement AS 'supportGroupInvolvement',
    Remarks as 'remarks',
    TransitionattritionReason as 'transitionAttritionReason',
    OutcomeDate AS 'outcomeDate'
    FROM
    ndwr.ndwr_otz_patient_visits
    JOIN
	    ndwr.ndwr_selected_site USING (SiteCode)
        group by VisitID
        LIMIT 0;

  ===========================================

   SELECT 
    PatientPK as 'patientPK',
    SiteCode as 'siteCode',
    PatientID as 'PatientID',
    SiteCode as 'facilityId', 
    FacilityName as 'facilityName',
    VisitID as 'visitID',
    VisitDate as 'visitDate',
    SessionNumber as 'sessionNumber',
    FirstSessionDate as 'dateOfFirstSession',
    PillCountAdherence as 'pillCountAdherence',
    CASE
     WHEN MMAS4_1 = 1 THEN 'YES'
     WHEN MMAS4_1 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmaS4_1',
    CASE
     WHEN MMAS4_2 = 1 THEN 'YES'
     WHEN MMAS4_2 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmaS4_2',
	CASE
     WHEN MMAS4_3 = 1 THEN 'YES'
     WHEN MMAS4_3 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmaS4_3',
    CASE
     WHEN MMAS4_4 = 1 THEN 'YES'
     WHEN MMAS4_4 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmaS4_4',
    CASE
     WHEN MMSA8_1 = 1 THEN 'YES'
     WHEN MMSA8_1 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmsA8_1',
    CASE
     WHEN MMSA4_2 = 1 THEN 'YES'
     WHEN MMSA4_2 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmsA8_2',
    CASE
     WHEN MMSA4_3 = 1 THEN 'YES'
     WHEN MMSA4_3 = 0 THEN 'NO'
     ELSE NULL
    END AS 'mmsA8_3',
    CASE
     WHEN MMSA4_4 = 0 THEN 'Never/Rarely'
     WHEN MMSA4_4 = 0.25 THEN 'Once in a while'
     WHEN MMSA4_4 = 0.5 THEN 'Sometimes'
     WHEN MMSA4_4 = 0.75 THEN 'Usually'
     WHEN MMSA4_4 = 1 THEN 'All the time'
     ELSE NULL
    END AS 'mmsA8_4',
    CASE
     WHEN MMSAScore >= 0 AND MMSAScore <= 0.25 THEN 'Good'
     WHEN MMSAScore >= 0.5 AND MMSAScore <= 2 THEN 'Inadequate'
     WHEN MMSAScore >= 3 AND MMSAScore <= 8 THEN 'Poor'
     ELSE 'Unknown'
    END AS 'mmsaScore',
    EACRecievedVL as 'eacRecievedVL',
    EACVL as 'eacvl',
    EACVLConcerns as 'eacvlConcerns',
    EACVLThoughts as 'eacvlThoughts',
    EACWayForward as 'eacWayForward',
    EACCognitiveBarrier as 'eacCognitiveBarrier',
    EACBehaviouralBarrier_1 as 'eacBehaviouralBarrier_1',
    EACBehaviouralBarrier_2 as 'eacBehaviouralBarrier_2',
    EACBehaviouralBarrier_3 as 'eacBehaviouralBarrier_3',
    EACBehaviouralBarrier_4 as 'eacBehaviouralBarrier_4',
    EACBehaviouralBarrier_5 as 'eacBehaviouralBarrier_5',
    EACEmotionalBarriers_1 as 'eacEmotionalBarriers_1',
    EACEmotionalBarriers_2 as 'eacEmotionalBarriers_2',
    EACEconBarrier_1 as 'eacEconBarrier_1',
    EACEconBarrier_2 as 'eacEconBarrier_2',
    EACEconBarrier_3 as 'eacEconBarrier_3',
    EACEconBarrier_4 as 'eacEconBarrier4',
    EACEconBarrier_5 as 'eacEconBarrier_5',
    EACEconBarrier_6 as 'eacEconBarrier_6',
    EACEconBarrier_7 as 'eacEconBarrier_7',
    EACEconBarrier_8 as 'eacEconBarrier_8',
    EACReviewImprovement as 'eacReviewImprovement',
    EACReviewMissedDoses as 'eacReviewMissedDoses',
    EACReviewStrategy as 'eacReviewStrategy',
    EACReferral as 'eacReferral',
    EACReferralApp as 'eacReferralApp',
    EACReferralExperience as 'eacReferralExperience',
    EACHomevisit as 'eacHomevisit',
    EACAdherencePlan as 'eacAdherencePlan',
    EACFollowupDate as 'eacFollowupDate'
FROM
    ndwr.ndwr_patient_eac
    JOIN
	    ndwr.ndwr_selected_site USING (SiteCode)
        group by VisitID;

  ===================================================

	SELECT 
    PatientPK,
    SiteCode,
    PatientID,
    FacilityId,
    FacilityName,
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
    END AS 'PHQ9_1',
    CASE
        WHEN PHQ9_2 = 0 THEN 'Not at all'
        WHEN PHQ9_2 = 1 THEN 'Several days'
        WHEN PHQ9_2 = 2 THEN 'More than half'
        WHEN PHQ9_3 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_2',
    CASE
        WHEN PHQ9_3 = 0 THEN 'Not at all'
        WHEN PHQ9_3 = 1 THEN 'Several days'
        WHEN PHQ9_3 = 2 THEN 'More than half'
        WHEN PHQ9_3 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_3',
    CASE
        WHEN PHQ9_4 = 0 THEN 'Not at all'
        WHEN PHQ9_4 = 1 THEN 'Several days'
        WHEN PHQ9_4 = 2 THEN 'More than half'
        WHEN PHQ9_4 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_4',
    CASE
        WHEN PHQ9_5 = 0 THEN 'Not at all'
        WHEN PHQ9_5 = 1 THEN 'Several days'
        WHEN PHQ9_5 = 2 THEN 'More than half'
        WHEN PHQ9_5 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_5',
    CASE
        WHEN PHQ9_6 = 0 THEN 'Not at all'
        WHEN PHQ9_6 = 1 THEN 'Several days'
        WHEN PHQ9_6 = 2 THEN 'More than half'
        WHEN PHQ9_6 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_6',
    CASE
        WHEN PHQ9_7 = 0 THEN 'Not at all'
        WHEN PHQ9_7 = 1 THEN 'Several days'
        WHEN PHQ9_7 = 2 THEN 'More than half'
        WHEN PHQ9_7 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_7',
    CASE
        WHEN PHQ9_8 = 0 THEN 'Not at all'
        WHEN PHQ9_8 = 1 THEN 'Several days'
        WHEN PHQ9_8 = 2 THEN 'More than half'
        WHEN PHQ9_8 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_8',
    CASE
        WHEN PHQ9_9 = 0 THEN 'Not at all'
        WHEN PHQ9_9 = 1 THEN 'Several days'
        WHEN PHQ9_9 = 2 THEN 'More than half'
        WHEN PHQ9_9 = 3 THEN 'Nearly everyday'
        ELSE NULL
    END AS 'PHQ9_9',
    CASE
        WHEN PHQ9Score >= 0 AND PHQ9Score <= 4 THEN 'Depression unlikely'
        WHEN PHQ9Score >= 5 AND PHQ9Score <= 9 THEN 'Mild depression'
        WHEN PHQ9Score >= 10 AND PHQ9Score <= 14 THEN 'Moderate depression'
        WHEN PHQ9Score >= 15 AND PHQ9Score <= 19 THEN 'Moderate severe depression'
        WHEN PHQ9Score >= 20 AND PHQ9Score <= 27 THEN 'Severe depression'
        ELSE NULL
    END AS 'DepressionAssesmentScore',
    PHQ9Rating AS 'PHQ_9_rating',
    DateCreated as 'Date_Created',
    DateCreated as 'Date_Last_Modified',
    NULL AS 'StatusDate',
    NULL AS 'Status'
FROM
    ndwr.ndwr_patient_depression_screening
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID;


  =====================================================

  SELECT 
    PatientPK as 'patientPK',
    SiteCode as 'siteCode',
    PatientID as 'patientID',
	SiteCode as 'facilityId', 
    FacilityName as 'facilityName',
    VisitID as 'visitID ',
    VisitDate as 'visitDate',
    OVCEnrollmentDate as 'ovcEnrollmentDate',
    RelationshiptoClient as 'relationshipToClient',
    EnrolleInCPIMS as 'enrolleInCPIMS',
    CPIMSUniqueidentifier as 'cpimsUniqueIdentifier',
    PartnerOfferingOVCServices AS 'partnerOfferingOVCServices',
    OVCExitReason as ' ovcExitReason',
    ExitDate as 'exitDate'
FROM
    ndwr.ndwr_ovc_patient_visits
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID
    LIMIT 0;

  =======================================================

    SELECT 
    PatientPK as 'patientPK',
    SiteCode as 'siteCode',
    PatientID as 'patientID',
    FacilityID as 'facilityId',
    FacilityName as 'facilityName',
    VisitID as 'visitID',
    VisitDate as 'visitDate',
    CASE
        WHEN OnTBDrugs = 1 THEN 'YES'
        WHEN OnTBDrugs = 0 THEN 'NO'
        ELSE NULL
    END AS 'onTBDrugs',
    CASE
        WHEN OnIPT = 1 THEN 'YES'
        WHEN OnIPT = 0 THEN 'NO'
        ELSE NULL
    END AS 'onIPT',
    CASE
        WHEN EverOnIPT = 1 THEN 'YES'
        WHEN EverOnIPT = 0 THEN 'NO'
        ELSE NULL
    END AS 'everOnIPT',
    CASE
        WHEN Cough = 1 THEN 'YES'
        WHEN Cough = 0 THEN 'NO'
        ELSE NULL
    END AS 'cough',
    CASE
        WHEN Fever = 1 THEN 'YES'
        WHEN Fever = 0 THEN 'NO'
        ELSE NULL
    END AS 'fever',
    CASE
        WHEN NoticeableWeightLoss = 1 THEN 'YES'
        WHEN NoticeableWeightLoss = 0 THEN 'NO'
        ELSE NULL
    END AS 'noticeableWeightLoss',
    CASE
        WHEN NightSweats = 1 THEN 'YES'
        WHEN NightSweats = 0 THEN 'NO'
        ELSE NULL
    END AS 'nightSweats',
    CASE
        WHEN Lethargy = 1 THEN 'YES'
        WHEN Lethargy = 0 THEN 'NO'
        ELSE NULL
    END AS 'lethargy',
    ICFActionTaken as 'icfActionTaken',
    TestResult as 'testResult',
    TBClinicalDiagnosis as 'tbClinicalDiagnosis',
    ContactsInvited as 'contactsInvited',
    CASE
        WHEN EvaluatedForIPT = 1 THEN 'YES'
        WHEN EvaluatedForIPT = 0 THEN 'NO'
        ELSE NULL
    END AS 'evaluatedForIPT',
    CASE
        WHEN StartAntiTBs = 1 THEN 'YES'
        WHEN StartAntiTBs = 0 THEN 'NO'
        ELSE NULL
    END AS 'startAntiTBs',
    TBRxStartDate as 'tbRxStartDate',
    TBScreening as 'tbScreening',
    IPTClientWorkUp as 'iptClientWorkUp',
    CASE
        WHEN StartIPT = 1 THEN 'YES'
        WHEN StartIPT = 0 THEN 'NO'
        ELSE NULL
    END AS 'startIPT',
    IndicationForIPT as 'indicationForIPT'
FROM
    ndwr.ndwr_patient_ipt_extract
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID
    LIMIT 0;

===============================================

  SELECT 
    PatientPK as 'patientPK',
    SiteCode as 'siteCode',
    PatientID as 'patientID',
    FacilityName as 'FacilityName',
    VisitID as 'visitID',
    VisitDate as 'visitDate',
    CASE
        WHEN IPV = 1 THEN 'Yes'
        WHEN IPV = 0 THEN 'NO'
        ELSE NULL
    END AS 'ipv',
    CASE
        WHEN PhysicalIPV = 1 THEN 'Yes'
        WHEN PhysicalIPV = 0 THEN 'NO'
        ELSE NULL
    END AS 'physicalIPV',
    CASE
        WHEN EmotionalIPV = 1 THEN 'Yes'
        WHEN EmotionalIPV = 0 THEN 'NO'
        ELSE NULL
    END AS 'emotionalIPV',
    CASE
        WHEN SexualIPV = 1 THEN 'Yes'
        WHEN SexualIPV = 0 THEN 'NO'
        ELSE NULL
    END AS 'sexualIPV',
    CASE
        WHEN IPVRelationship = 1 THEN 'Yes'
        WHEN IPVRelationship = 0 THEN 'NO'
        ELSE NULL
    END AS 'ipvRelationship',
    DateCreated
FROM
    ndwr.ndwr_gbv_screening
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID
    LIMIT 0;
    

=================================================

SELECT 
    PatientPK as 'PatientPK',
    SiteCode as 'SiteCode',
    PatientID as 'PatientID',
    SiteCode as 'FacilityId',
    FacilityName as 'FacilityName',
    VisitID as 'VisitID',
    VisitDate as 'VisitDate',
    CASE
        WHEN DrinkAlcohol = 1090 THEN 'Never'
        WHEN DrinkAlcohol = 1091 THEN 'Monthly or less'
        WHEN DrinkAlcohol = 1092 THEN '2 to 4 times a month'
        WHEN DrinkAlcohol = 1093 THEN '2 to 3 times a week'
        WHEN DrinkAlcohol IN (1094 , 1095) THEN '4 or More Times a Week'
        ELSE NULL
    END AS 'DrinkingAlcohol',
    NULL AS 'Smoking',
    NULL AS 'DrugUse',
    NULL AS 'Status',
    NULL AS 'StatusDate'
FROM
    ndwr.ndwr_drug_alcohol_screening
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID
    LIMIT 0;

=============================================================

SELECT 
    PatientPK,
    SiteCode,
    SiteCode as 'FacilityId',
    PatientID,
    FacilityName,
    Emr,
    Project,
    case
      WHEN PartnerPersonID IS NULL THEN ''
      ELSE PartnerPersonID
    end as 'PartnerPersonID',
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
    END AS 'PnsApproach',
    DateCreated as 'Date_Created',
    DateCreated as 'Date_Last_Modified',
    NULL AS 'Status'
FROM
    ndwr.ndwr_patient_contact_listing c
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID
    LIMIT 0;

    ======================================================

	SELECT 
    PatientPK as 'PatientPK',
    SiteCode as 'SiteCode',
    CASE
        WHEN PatientID IS NULL THEN PatientPK
        ELSE PatientID
    END AS 'PatientID',
    FacilityID AS 'FacilityId',
    Emr,
    Project,
    FacilityName as 'FacilityName',
    VisitID as 'VisitID',
    VisitDate as 'VisitDate',
    ChronicIllness as 'ChronicIllness',
    ChronicOnsetDate as 'ChronicOnsetDate',
    CASE
        WHEN knownAllergies = 1 THEN 'YES'
        WHEN knownAllergies = 0 THEN 'NO'
        ELSE NULL
    END AS 'knownAllergies',
    AllergyCausativeAgent as 'AllergyCausativeAgent',
    AllergicReaction as 'AllergicReaction',
    AllergySeverity as 'AllergySeverity',
    AllergyOnsetDate as 'AllergyOnsetDate',
    Skin as 'Skin',
    NULL AS 'Status',
    NULL AS 'StatusDate',
    Eyes as 'Eyes',
    ENT as 'ENT',
    Chest as 'Chest',
    CVS as 'CVS',
    Abdomen as 'Abdomen',
    CNS as 'CNS',
    Genitourinary as 'Genitourinary',
    DateCreated AS 'Date_Created',
    DateCreated as 'Date_Last_Modified'
FROM
    ndwr.ndwr_patient_allergies_chronic_illness
        JOIN
    ndwr.ndwr_selected_site USING (SiteCode)
    group by VisitID
    LIMIT 0;

  =====================================================



  
		
