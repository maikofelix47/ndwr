use ndwr;
DELIMITER $$
CREATE  PROCEDURE `build_ndwr_Queue`()
BEGIN

 DECLARE selectMflCode INT DEFAULT 0;
 DECLARE selectedPeriod date;        
 Select reporting_period from ndwr.mfl_period limit 1 into selectedPeriod;
 Select mfl_code from ndwr.mfl_period limit 1 into selectMflCode;
 # Clearing historical data from the tables 
 
 delete from ndwr.ndwr_baseline_build_queue;
             
 replace into ndwr.ndwr_baseline_build_queue 
 select person_id from etl.hiv_monthly_report_dataset_frozen
 where enddate=selectedPeriod and location_id in(select location_id from ndwr.mfl_codes where mfl_code=selectMflCode) ;
 
 
  END$$
DELIMITER ;