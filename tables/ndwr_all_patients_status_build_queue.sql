use ndwr;
CREATE TABLE ndwr_all_patient_status_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX status_person_id (person_id)
);