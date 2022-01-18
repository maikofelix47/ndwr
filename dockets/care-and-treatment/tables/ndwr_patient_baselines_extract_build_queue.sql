use ndwr;
CREATE TABLE ndwr_patient_baselines_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX baseline_person_id (person_id)
);