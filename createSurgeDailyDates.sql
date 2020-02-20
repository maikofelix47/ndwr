CREATE  PROCEDURE `createSurgeDailyDates`(dateStart DATE,dateEnd DATE)
BEGIN

CREATE TABLE IF NOT EXISTS surge_days (
  _date DATETIME,
  PRIMARY KEY _date (_date)
 );
 
 while dateStart<=dateEnd do
 REPLACE INTO surge_days(_date) VALUE (dateStart);
 set dateStart=date_add(dateStart,INTERVAL 1 DAY);
 
 END WHILE;
 END