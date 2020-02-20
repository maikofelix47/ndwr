CREATE  PROCEDURE `buildNDWRSubQueues`()
BEGIN
 DECLARE queueSize INT DEFAULT 0;
 DECLARE selectMflCode INT DEFAULT 0;
 DECLARE selectedPeriod date;        
 Select reporting_period from ndwr.mfl_period limit 1 into selectedPeriod;
 Select mfl_code from ndwr.mfl_period limit 1 into selectMflCode;
 # Clearing historical data from the tables 
 
              delete from ndwr.ndwr_baseline_queue_12;
              delete from ndwr.ndwr_baseline_queue_11;
              delete from ndwr.ndwr_baseline_queue_10;
              delete from ndwr.ndwr_baseline_queue_9;
              delete from ndwr.ndwr_baseline_queue_8;
              delete from ndwr.ndwr_baseline_queue_7;
              delete from ndwr.ndwr_baseline_queue_6;
              delete from ndwr_baseline_queue_5;
              delete from ndwr_baseline_queue_4;
              delete from ndwr.ndwr_baseline_queue_3;
              delete from ndwr.ndwr_baseline_queue_2;
              delete from ndwr.ndwr_baseline_queue; 
             
              # Clear data for the previous facility loaded
 
              delete from ndwr.ndwr_patient_labs;
              delete from ndwr.ndwr_art_patients;
              delete from ndwr.ndwr_patient_status;
              delete from ndwr_all_patient_visits;
              delete from ndwr_all_patients;
              delete from ndwr.ndwr_patient_pharmacy;
              delete from ndwr.ndwr_patient_status;
              delete from ndwr.ndwr_vitals; 
              delete FROM ndwr.patient_base_line;
              delete FROM ndwr.ndwr_base_line;
 			 delete from base_temp_1;
 			 delete from progress;
 
 
 replace into ndwr.ndwr_baseline_queue 
 select person_id from etl.hiv_monthly_report_dataset_frozen
 where enddate=selectedPeriod and location_id in(select location_id from ndwr.mfl_codes where mfl_code=selectMflCode) ;
 SELECT Floor(count(*)/12) from ndwr.ndwr_baseline_queue  INTO queueSize; 
 
 replace into ndwr_baseline_queue_12  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_12);
 
 replace into ndwr_baseline_queue_11  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_11);
 
 replace into ndwr_baseline_queue_10  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_10);
 
 replace into ndwr_baseline_queue_9  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_9);
 
 replace into ndwr_baseline_queue_8  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_8);
 
 replace into ndwr_baseline_queue_7  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_7);
 
 
 replace into ndwr_baseline_queue_6  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_6);
 
 replace into ndwr_baseline_queue_5  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_5);
 
 replace into ndwr_baseline_queue_4  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_4);
 
 replace into ndwr_baseline_queue_3  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_3);
 
 replace into ndwr_baseline_queue_2  SELECT person_id from ndwr_baseline_queue limit queueSize;
 delete from ndwr_baseline_queue where person_id in(Select person_id from ndwr_baseline_queue_2);
 
  END