create or replace
FUNCTION  string2timestamp(p_time_string VARCHAR2) RETURN
  TIMESTAMP
IS
   r_timestamp            TIMESTAMP;
   v_day_increment        PLS_INTEGER  := 0;
   v_hour_part            PLS_INTEGER;
   v_rest_of_time_string  VARCHAR2(6);
BEGIN
   v_hour_part           := TO_NUMBER(SUBSTR(p_time_string, 1, 2));
   v_rest_of_time_string := SUBSTR(p_time_string, 3, 6);
   IF  v_hour_part > 23
   THEN
     v_day_increment := 1;
     v_hour_part     := MOD(v_hour_part,24);
   END IF;
   r_timestamp := TO_TIMESTAMP(TO_CHAR(v_hour_part)||v_rest_of_time_string, 'HH24:MI:SS') + v_day_increment;
   Return  R_Timestamp;
END;