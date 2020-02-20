CREATE  PROCEDURE `createWeeDates`(dateStart DATE,dateEnd DATE)
BEGIN
#set @w=1; set @i=0;
DECLARE w int default 1;
declare i INT DEFAULT 0;
while dateStart<=dateEnd do
set i=i+1;	
if(MOD(i, 7)=0) then
set w=w+1;
end if;
INSERT INTO reporting_week(Dates) VALUE (dateStart,concat('week_',w));
set dateStart=date_add(dateStart,INTERVAL 1 DAY);


END WHILE;
END