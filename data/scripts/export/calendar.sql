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
  SERVICE_ID ||','|| 
  Monday ||','|| 
  Tuesday ||','||
  Wednesday  ||','||
  Thursday  ||','||
  Friday ||','||
  Saturday  ||','||
  Sunday  ||','||
  To_Char(Start_Date,'YYYYMMDD')  ||','|| 
  To_Char(End_Date,'YYYYMMDD') 
  FROM @TABLE_NAME@
ORDER BY SERVICE_ID;
SPOOL OFF;
QUIT