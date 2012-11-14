DROP TABLE @TABLE_NAME@;
CREATE TABLE @TABLE_NAME@ AS (
SELECT 
  TRIP_ID,
  ROUTE_ID,  
  DIRECTION_ID, 
  SERVICE_ID 
 FROM @TMP_TRIPS_TABLE@
);
ALTER TABLE @TABLE_NAME@ ADD PRIMARY KEY (TRIP_ID);
QUIT