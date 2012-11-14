SET COLSEP ',';
SET MARKUP HTML OFF;
SET NEWPAGE 0;
SET LINESIZE 150;
SET PAGESIZE 0;
SET SPACE 0;
SET ECHO OFF;
SET FEEDBACK OFF;
SET VERIFY OFF;
SET termout OFF;
set heading ON;
SET TRIMSPOOL ON;
SPOOL "@OUTPUT_FILE@" append;
SELECT TRIP_ID ||','||
 Timestamp2string(ARRIVAL_TIME,'YYYY-MM-DD') ||','||
 Timestamp2string(DEPARTURE_TIME,'YYYY-MM-DD') ||','||
 STOP_ID ||','|| 
 STOP_SEQUENCE
FROM @TABLE_NAME@ ORDER BY TRIP_ID, STOP_SEQUENCE;
SPOOL OFF;
QUIT
