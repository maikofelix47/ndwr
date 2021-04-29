use ndwr;
CREATE TABLE ndwr_patient_depression_screening_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_depression_screening (person_id)
);