CREATE  PROCEDURE `setReportingPeriod`(IN selectedMFLCode INT,IN selectedPeriod date)
BEGIN
	
		 set @dyn_sql=concat('update ndwr.mfl_period set mfl_code="', selectedMFLCode, '"', ' , reporting_period="', selectedPeriod, '"');
         select @dyn_sql;
		 PREPARE s1 from @dyn_sql; 
		 EXECUTE s1; 
		 DEALLOCATE PREPARE s1;
END