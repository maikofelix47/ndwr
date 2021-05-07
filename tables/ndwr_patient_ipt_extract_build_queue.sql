use ndwr;
CREATE TABLE ndwr_patient_ipt_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_ipt (person_id)
);