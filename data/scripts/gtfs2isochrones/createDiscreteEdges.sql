CREATE SEQUENCE TMP_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1;

DROP TABLE @TABLE_NAME@;
CREATE TABLE @TABLE_NAME@ AS 
 SELECT TMP_SEQ.NEXTVAL ID, A.* FROM (
	SELECT  
	  DEP.STOP_ID SOURCE, ARR.STOP_ID TARGET, T.ROUTE_ID ROUTE_ID
	FROM @STOPTIMES_TABLE@ DEP, @STOPTIMES_TABLE@ ARR, @TRIPS_TABLE@ T 
	WHERE   
	  DEP.TRIP_ID=ARR.TRIP_ID 
	  AND DEP.TRIP_ID=T.TRIP_ID 
	  AND DEP.STOP_SEQUENCE = (
	   SELECT MAX(S2.STOP_SEQUENCE) FROM @STOPTIMES_TABLE@ S2 WHERE S2.TRIP_ID=ARR.TRIP_ID AND 
	   S2.STOP_SEQUENCE<ARR.STOP_SEQUENCE
	  )
	GROUP BY DEP.STOP_ID, ARR.STOP_ID, T.ROUTE_ID HAVING DEP.STOP_ID!=ARR.STOP_ID
 ) A;
DROP SEQUENCE TMP_SEQ;
QUIT