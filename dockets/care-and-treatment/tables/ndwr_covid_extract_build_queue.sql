use ndwr;
CREATE TABLE ndwr_covid_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_covid (person_id)
);