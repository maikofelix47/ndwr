use ndwr;
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