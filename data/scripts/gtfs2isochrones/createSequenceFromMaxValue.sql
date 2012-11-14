-- creates a sequence by taking the max column value of the passed table
-- Placeholder: 
-- @SEQUENCE_NAME@ the name of the sequence
-- @COLUMN_NAME@ the name of the column
-- @TABLE_NAME@ the name of the table
DROP SEQUENCE @SEQUENCE_NAME@;
Column S New_Val Inc;
SELECT MAX(@COLUMN_NAME@)+1 S FROM @TABLE_NAME@;
CREATE SEQUENCE @SEQUENCE_NAME@ START WITH &inc;
QUIT