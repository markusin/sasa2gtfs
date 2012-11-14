DROP TABLE @EDGE_TABLE@;

CREATE TABLE @EDGE_TABLE@ AS (
  SELECT ID,SOURCE,TARGET,LENGTH,GEOMETRY,0 EDGE_MODE FROM @PED_EDGE_TABLE@
);

ALTER TABLE @EDGE_TABLE@ ADD PRIMARY KEY (ID);

ALTER TABLE @EDGE_TABLE@ ADD (
  ROUTE_ID NUMBER(5,0), 
  SOURCE_OUTDEGREE NUMBER(5,0), SOURCE_C_OUTDEGREE NUMBER(5,0),
  TARGET_INDEGREE NUMBER(5,0), TARGET_C_INDEGREE NUMBER(5,0)
);

DROP INDEX IDX_SOURCE_@EDGE_TABLE@;
CREATE INDEX IDX_SOURCE_@EDGE_TABLE@ ON @EDGE_TABLE@(SOURCE);
DROP INDEX IDX_TARGET_@EDGE_TABLE@;
CREATE INDEX IDX_TARGET_@EDGE_TABLE@ ON @EDGE_TABLE@(TARGET);

QUIT