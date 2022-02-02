use ndwr;
CREATE TABLE ndwr_mnch_mother_baby_pair_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX mnch_mother_baby_person_id (person_id)
);