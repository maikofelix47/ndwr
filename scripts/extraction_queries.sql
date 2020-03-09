#tab 1 first connection
call setReportingPeriod(13528
,'2020-01-31');
call setReportingPeriod(14555
,'2020-02-29');
call `ndwr`.`buildNDWRSubQueues`();

#tab 2 first connection
call `ndwr`.`buildNDWRFacilityData`();


#tab 2 first connection
call buildNDWR_QueueData_2();

#tab 3 first connection
call buildNDWR_QueueData_3();

#tab 4 first connection
call buildNDWR_QueueData_4();
.....
12
#tab 12 first connection
call buildNDWR_QueueData_12();

#####################
select 1 Queue, count(*) patients from ndwr_baseline_queue union
select 2, count(*) from ndwr_baseline_queue_2 union
select 3, count(*) from ndwr_baseline_queue_3 union
select 4,count(*) from ndwr_baseline_queue_4 union
select 5, count(*) from ndwr_baseline_queue_5 union
select 6, count(*) from ndwr_baseline_queue_6 union
select 7, count(*) from ndwr_baseline_queue_7 union
select 8, count(*) from ndwr_baseline_queue_8 union
select 9, count(*) from ndwr_baseline_queue_9 union
select 10, count(*) from ndwr_baseline_queue_10 union
select 11, count(*) from ndwr_baseline_queue_11 union
select 12, count(*) from ndwr_baseline_queue_12