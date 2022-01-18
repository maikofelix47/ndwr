use ndwr;
CREATE TABLE ndwr_drug_alcohol_screening_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_das (person_id)
);