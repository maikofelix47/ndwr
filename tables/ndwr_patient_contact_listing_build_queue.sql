use ndwr;
CREATE TABLE ndwr_patient_contact_listing_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_cl (person_id)
);