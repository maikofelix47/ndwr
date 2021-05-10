use ndwr;
CREATE TABLE ndwr_ovc_patient_visits_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_ovcv (person_id)
);