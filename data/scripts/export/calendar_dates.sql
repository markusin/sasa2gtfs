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
	service_id ||','|| 
	to_char(datum,'YYYYMMDD') ||','|| 
	Exception_Type  
 FROM @TABLE_NAME@;
SPOOL OFF;
QUIT