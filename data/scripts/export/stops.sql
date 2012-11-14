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
set heading OFF;
SET TRIMSPOOL ON;
SPOOL "@OUTPUT_FILE@" append;
SELECT 
	Stop_Id ||',"'|| 
	Description||'",'|| 
	LATITUDE ||','||
	LONGITUDE 
FROM @TABLE_NAME@ ORDER BY stop_id;
SPOOL OFF;
QUIT