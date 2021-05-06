use ndwr;
CREATE TABLE ndwr_patient_allergies_chronic_illness_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_aci (person_id)
);