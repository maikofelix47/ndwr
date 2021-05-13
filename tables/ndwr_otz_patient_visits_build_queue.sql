use ndwr;
CREATE TABLE ndwr_otz_patient_visits_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_otz (person_id)
);