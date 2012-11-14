DROP TABLE @TABLE_NAME@;
CREATE TABLE @TABLE_NAME@ (
 	TRIP_ID NUMBER NOT NULL 
, OPTIONAL_LINIEN_ID NUMBER
, STOP_ID NUMBER NOT NULL 
, STOP_SEQUENCE NUMBER NOT NULL
, DEPARTURE_TIME TIMESTAMP(6)
, ARRIVAL_TIME TIMESTAMP(6)
, CONSTRAINT @TABLE_NAME@_PK PRIMARY KEY (TRIP_ID, STOP_ID, STOP_SEQUENCE) ENABLE 
);
QUIT