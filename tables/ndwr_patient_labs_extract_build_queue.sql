use ndwr;
CREATE TABLE ndwr_patient_labs_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX labs_person_id (person_id)
);