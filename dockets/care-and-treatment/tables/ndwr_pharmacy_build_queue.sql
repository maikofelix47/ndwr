use ndwr;
CREATE TABLE ndwr_pharmacy_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX pharmacy_person_id (person_id)
);