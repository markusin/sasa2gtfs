-- we make a copy since we do not want to work on the orgin table
CREATE TABLE @TABLE_NAME@ AS SELECT * FROM @STOPTIMES_TABLE@;
commit;

UPDATE @TABLE_NAME@ SET ARRIVAL_TIME=NULL,TIME_A=NULL
WHERE (STOP_SEQUENCE,TRIP_ID) IN (
 SELECT MIN(STOP_SEQUENCE),TRIP_ID FROM @TABLE_NAME@ GROUP BY TRIP_ID
);
commit;
UPDATE @TABLE_NAME@ SET DEPARTURE_TIME=NULL,TIME_D=NULL
WHERE (STOP_SEQUENCE,TRIP_ID) IN (
 SELECT MAX(STOP_SEQUENCE),TRIP_ID FROM @TABLE_NAME@ GROUP BY TRIP_ID
);
commit;