use ndwr;
CREATE TABLE ndwr_all_patient_visits_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX visits_person_id (person_id)
);