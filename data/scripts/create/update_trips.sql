
DROP TABLE T_@ROUTE_TABLE@;
CREATE TABLE  T_@ROUTE_TABLE@ 
 AS 
  SELECT 
  	ROUTE_ID, ROUTE_DESC, ROUTE_LONG_NAME, ROUTE_SHORT_NAME, ROUTE_TYPE,
  	OPTIONAL_LOCATION,OPTIONAL_ROUTE_DESC_DE,OPTIONAL_ROUTE_NAME, OPTIONAL_ROUTE_URL, AGENCY_ID
   FROM ( 
    SELECT @ROUTE_TABLE@.*, row_number() over (partition BY OPTIONAL_ROUTE_NAME,OPTIONAL_LOCATION ORDER BY rowid) rn FROM @ROUTE_TABLE@)
   WHERE rn = 1;
   
SELECT count(*) from T_@ROUTE_TABLE@;
   
DROP TABLE @ROUTE_TABLE@;

ALTER TABLE T_@ROUTE_TABLE@ RENAME TO @ROUTE_TABLE@;

UPDATE @TRIP_TABLE@ T SET T.Route_Id=(
	SELECT R.Route_Id FROM @ROUTE_TABLE@ R
 	WHERE
		REPLACE(LTRIM(RTRIM(R.OPTIONAL_ROUTE_NAME)),CHR(13))=REPLACE(LTRIM(RTRIM(T.ROUTE_NAME)),CHR(13)) AND
		REPLACE(LTRIM(RTRIM(R.OPTIONAL_LOCATION)),CHR(13))=REPLACE(LTRIM(RTRIM(T.LOCATION)),CHR(13))
);
QUIT
