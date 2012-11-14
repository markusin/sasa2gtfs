SET MARKUP HTML OFF;
SET NEWPAGE 0;
SET LINESIZE 150;
SET PAGESIZE 0;
SET SPACE 0;
SET ECHO OFF;
SET FEEDBACK OFF;
SET VERIFY OFF;
set heading OFF;
set termout OFF;
SET TRIMSPOOL ON;
SPOOL "@OUTPUT_FILE@" append;
SELECT ROUTE_ID ||','|| SERVICE_ID ||','|| TRIP_ID ||','|| 
Case DIRECTION_ID 
  When 'A' Then 0
  Else 1 
End AS DIRECTION 
FROM @TABLE_NAME@ ORDER BY TRIP_ID;
SPOOL OFF;
QUIT