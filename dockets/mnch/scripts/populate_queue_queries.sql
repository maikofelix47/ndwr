set @startDate := '2013-01-01';
replace into ndwr.ndwr_mnch_patient_extract_build_queue(
SELECT DISTINCT
    pp.patient_id
FROM
    amrs.patient_program pp
        JOIN
    etl.flat_appointment fa ON (fa.person_id = pp.patient_id
        AND fa.is_clinical = 1
        AND fa.next_clinical_encounter_datetime IS NULL)
WHERE
    pp.program_id IN (4 , 29)
        AND fa.program_id IN (4 , 29)
        AND pp.voided = 0
        AND fa.date_created >= @startDate
        group by pp.patient_id
    );