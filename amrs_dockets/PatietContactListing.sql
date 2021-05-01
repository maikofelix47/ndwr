use ndwr;
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
    ndwr.ndwr_patient_contact_listing c