use ndwr;
CREATE TABLE ndwr_mnch_patient_hei_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX mnch_patient_hei_person_id (person_id)
);