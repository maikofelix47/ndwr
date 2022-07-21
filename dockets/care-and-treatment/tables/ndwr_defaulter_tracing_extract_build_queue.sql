use ndwr;
CREATE TABLE ndwr_defaulter_tracing_extract_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX defaulter_tracing_person_id (person_id)
);