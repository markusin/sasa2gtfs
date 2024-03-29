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
SELECT 
	ROUTE_ID ||',"'|| 
	Rtrim(ROUTE_SHORT_NAME) ||'","'|| 
	Rtrim(ROUTE_LONG_NAME) ||'","'||  Rtrim(ROUTE_DESC) ||'",'||
	ROUTE_TYPE
FROM @TABLE_NAME@ ORDER BY ROUTE_ID ASC;
SPOOL OFF;
QUIT