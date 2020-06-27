use ndwr;
CREATE TABLE ndwr_patient_adverse_events_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id (person_id),
);