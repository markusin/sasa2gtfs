DROP TABLE @TABLE_NAME@;
CREATE TABLE @TABLE_NAME@ (
	TRIP_ID NUMBER NOT NULL 
, ROUTE_ID NUMBER  
, OPTIONAL_FAHRTEN_ID NUMBER 
, OPTIONAL_LINIEN_ID NUMBER 
, LOCATION VARCHAR2(20 BYTE) 
, ROUTE_NAME VARCHAR2(20 BYTE) 
, LINIENBESCHREIBUNG_I VARCHAR2(50 BYTE) 
, LINIENBESCHREIBUNG_D VARCHAR2(50 BYTE) 
, DIRECTION_ID VARCHAR2(1 CHAR) 
, GUELTIGKEIT_AB DATE 
, KALENDER Varchar2(370 Char)
, CONSTRAINT @TABLE_NAME@_PK PRIMARY KEY (TRIP_ID) ENABLE
);
QUIT
