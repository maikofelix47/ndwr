CREATE  PROCEDURE `createWeekDates`(dateStart DATE,dateEnd DATE)
BEGIN
  delete from reporting_week;
  while dateStart<=dateEnd do
  INSERT INTO reporting_week(week_date,week) VALUE (dateStart,yearweek(dateStart));
  set dateStart=date_add(dateStart,INTERVAL 1 DAY);
  
  
  END WHILE;
  truncate surge_week;
  replace into surge_week
   select min(week_date) start_date, max(week_date) end_date,week from reporting_week group by week;
  END