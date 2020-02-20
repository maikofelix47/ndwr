CREATE  PROCEDURE `moveToNewNDWRQueue`(
 IN source VARCHAR(100),
 IN queue VARCHAR(100),
 IN queueSize INT
 )
BEGIN
		 set @replaceSql:=concat("replace into ",queue,"  SELECT person_id from ",source,"  limit ",queueSize);
		 set @deleteSql:=concat("delete from ",source," where person_id in(Select person_id from ",queue,")");
select @replaceSql rpl,@deleteSql dlt;

		 PREPARE stmt1 FROM @replaceSql;
		 EXECUTE stmt1;
		 DEALLOCATE PREPARE stmt1; 
		 
		 PREPARE stmt2 FROM @deleteSql;
		 EXECUTE stmt2;
		 DEALLOCATE PREPARE stmt2; 
 
 END