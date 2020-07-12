use ndwr;
CREATE TABLE ndwr_all_patients_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX all_patients_person_id (person_id)
);