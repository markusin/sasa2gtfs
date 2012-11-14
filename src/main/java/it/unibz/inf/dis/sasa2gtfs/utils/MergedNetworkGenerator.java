/**
 * 
 */
package it.unibz.inf.dis.sasa2gtfs.utils;

import it.unibz.inf.dis.db.DBVendor;
import it.unibz.inf.dis.db.JDBCConnector;
import it.unibz.inf.dis.sasa2gtfs.network.components.Link;
import it.unibz.inf.dis.sasa2gtfs.network.components.LinkCluster;
import it.unibz.inf.dis.sasa2gtfs.network.components.OffsetEntry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import oracle.jdbc.OraclePreparedStatement;

/**
 * <p>
 * The <code>MergedNetworkGenerator</code> class
 * </p>
 * <p>
 * Copyright: 2006 - 2009 <a href="http://www.inf.unibz.it/dis">Dis Research Group</a>
 * </p>
 * <p>
 * Domenikanerplatz - Bozen, Italy.
 * </p>
 * <p>
 * </p>
 * 
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 2.2
 */
public class MergedNetworkGenerator {

  private JDBCConnector connector;

  /**
   * <p>
   * Constructs a(n) <code>MergedNetworkGenerator</code> object.
   * </p>
   * 
   * @param uname
   * @param passw
   * @param host
   * @param sid
   * @param port
   */
  public MergedNetworkGenerator(DBVendor database, String uname, String passw, String host, String sid, String port) {
    connector = new JDBCConnector(database, uname, passw, host, sid, port);
  }

  private void createNodeTable(String destNodesTable, String destLinkTable) {
    System.out.println("Start: node creation on table " + destNodesTable);
    dropTable(destNodesTable);
    Connection connection = connector.getConnection();
    StringBuilder b = new StringBuilder();
    Statement statement = null;

    b.append("CREATE TABLE ").append(destNodesTable).append(" AS ");
    b.append("SELECT SOURCE NODE_ID, SDO_LRS.GEOM_SEGMENT_START_PT(GEOMETRY) GEOMETRY FROM ").append(destLinkTable);
    b.append(" UNION ALL ");
    b.append("SELECT TARGET, SDO_LRS.GEOM_SEGMENT_END_PT(GEOMETRY) FROM ").append(destLinkTable);

    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }

    b.delete(0, b.length());
    b.append("DELETE FROM ").append(destNodesTable).append(" WHERE rowid IN(");
    b.append("SELECT rowid FROM (");
    b.append("SELECT N.NODE_ID,N.GEOMETRY, Row_Number() Over (Partition BY N.NODE_ID ORDER BY Rowid) RN FROM ");
    b.append(destNodesTable).append(" N) WHERE RN > 1)");
    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
    b.delete(0, b.length());
    b.append("UPDATE ").append(destNodesTable)
        .append(" SET GEOMETRY=" + "SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.CONVERT_TO_LRS_GEOM(GEOMETRY))");

    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
    System.out.println("End: node creation on table " + destNodesTable);
  }

  private void mergeLinks(String destLinkTable, String busLinkTable, String tripsTable, String linkSequence) {
    System.out.println("Start: merging links");
    Connection connection = connector.getConnection();

    StringBuilder b = new StringBuilder();
    Statement statement = null;
    b.append("INSERT INTO ").append(destLinkTable).append("(ID, SOURCE, TARGET, ROUTE_ID, EDGE_MODE) ");
    b.append("SELECT " + linkSequence + ".NEXTVAL, L.SOURCE, L.TARGET, L.ROUTE_ID,1 FROM ");
    b.append(busLinkTable).append(" L");

    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
    setOutgoingLinks(destLinkTable);
    // dropTable(busLinkTable);
    System.out.println("End: merging links");
  }

  private void setOutgoingLinks(String destLinkTable) {
    System.out.println("Start: outgoing links setting");
    String tmpTableName = "TMP_LINKS_OUTGOING";
    Statement statement = null;

    dropTable(tmpTableName);

    Connection connection = connector.getConnection();

    try {
      statement = connection.createStatement();
      statement.executeUpdate("CREATE INDEX IDX_" + destLinkTable + " ON " + destLinkTable + " (SOURCE)");
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }

    StringBuilder b = new StringBuilder();
    b.append("CREATE TABLE ").append(tmpTableName).append(" AS (");
    b.append(" SELECT L1.ID, COUNT(*) AS SOURCE_OUTDEGREE FROM ");
    b.append(destLinkTable).append(" L1, ").append(destLinkTable).append(" L2 ");
    b.append("WHERE L1.SOURCE=L2.SOURCE GROUP BY L1.ID, L1.SOURCE, L1.TARGET)");
    try {
      statement = connection.createStatement();
      statement.executeUpdate(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }

    // create index
    try {
      statement = connection.createStatement();
      statement.executeUpdate("ALTER TABLE " + tmpTableName + " ADD PRIMARY KEY (ID)");
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }

    b.delete(0, b.length());
    b.append("UPDATE ").append(destLinkTable).append(" L SET L.SOURCE_OUTDEGREE=(");
    b.append(" SELECT T.SOURCE_OUTDEGREE FROM ").append(tmpTableName).append(" T WHERE T.ID= L.ID)");
    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
    dropTable(tmpTableName);
    // same for ped outgoinglinks
    connection = connector.getConnection();
    b = new StringBuilder();
    b.append("CREATE TABLE ").append(tmpTableName).append(" AS (");
    b.append(" SELECT L1.ID, COUNT(*) AS SOURCE_COUTDEGREE FROM ");
    b.append(destLinkTable).append(" L1, ").append(destLinkTable).append(" L2 ");
    b.append("WHERE L1.SOURCE=L2.SOURCE AND L1.EDGE_MODE=0 AND L2.EDGE_MODE=0 " + "GROUP BY L1.ID, L1.SOURCE, L1.TARGET)");
    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }

    b.delete(0, b.length());
    b.append("UPDATE ").append(destLinkTable).append(" L SET L.SOURCE_COUTDEGREE=(");
    b.append(" SELECT T.SOURCE_COUTDEGREE FROM ").append(tmpTableName).append(" T WHERE T.ID= L.ID)");
    try {
      statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
    dropTable(tmpTableName);
    System.out.println("End: outgoing links setting");
  }

  private void dropTable(String tableName) {
    Connection connection = connector.getConnection();
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.execute("DROP TABLE " + tableName);
      connection.commit();
    } catch (SQLException e) {
      // e.printStackTrace();
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
  }

  private void createSequence(String sequenceName, int minValue) {
    Connection connection = connector.getConnection();
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.execute("CREATE SEQUENCE " + sequenceName + " MINVALUE " + minValue + " INCREMENT BY 1 START WITH "
          + minValue);
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void createSequenceStartingWithMaxValues(String sequenceName, String tableName, String columnName) {
    dropSequence(sequenceName);
    Connection connection = connector.getConnection();
    Statement statement = null;
    try {
      int minValue = Integer.MIN_VALUE;
      statement = connection.createStatement();
      ResultSet rSet = statement.executeQuery("SELECT MAX(" + columnName + ")+1 FROM " + tableName);
      if (rSet.next()) {
        minValue = rSet.getInt(1);
      }
      System.out.println("Initial value of sequence is: " + minValue);

      statement = connection.createStatement();
      statement.executeUpdate("CREATE SEQUENCE " + sequenceName + " MINVALUE " + minValue
          + " INCREMENT BY 1 START WITH " + minValue);
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void dropSequence(String sequenceName) {
    Connection connection = connector.getConnection();
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.execute("DROP SEQUENCE " + sequenceName);
      connection.commit();
    } catch (SQLException e) {
    } finally {
      if (statement != null)
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
  }

  private void createInvertedLinks(String destLinkTable, int linkId, String sequence) {
    System.out.println("Start: inserting reverted links");
    StringBuilder b = new StringBuilder();
    Connection connection = connector.getConnection();

    b.append("INSERT INTO ").append(destLinkTable).append(" (ID, SOURCE, TARGET, LENGTH, GEOMETRY,EDGE_MODE) ");
    b.append("SELECT ")
        .append(sequence)
        .append(
            ".NEXTVAL,L.TARGET,L.SOURCE,L.LENGTH,SDO_LRS.REVERSE_GEOMETRY(L.GEOMETRY),0 FROM " + destLinkTable
                + " L");
    try {
      Statement statement = connection.createStatement();
      statement.execute(b.toString());
      connection.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    System.out.println("End: inserting reverted links");
  }

  public int copyTables(String srcLinkTable, String destLinkTable, String dim1, String dim2, String srid) {
    try {
      connector.getStatement().execute("DROP TABLE " + destLinkTable + " CASCADE CONSTRAINTS");
      connector.getConnection().commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    StringBuilder b = new StringBuilder();
    b.append("CREATE TABLE ").append(destLinkTable);
    b.append(" AS (SELECT * FROM ").append(srcLinkTable).append(" )");

    try {
      connector.getStatement().execute(b.toString());
      connector.getConnection().commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    try {
      connector.getStatement().execute("ALTER TABLE " + destLinkTable + " ADD PRIMARY KEY (ID)");
      connector.getConnection().commit();
      connector
          .getStatement()
          .execute(
              "ALTER TABLE "
                  + destLinkTable
                  + " ADD (EDGE_MODE NUMBER(2,0), ROUTE_ID NUMBER(5,0), SOURCE_OUTDEGREE NUMBER(3,0), SOURCE_COUTDEGREE NUMBER(2,0) )");
      connector.getConnection().commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    try {
      connector.getStatement().executeUpdate("UPDATE " + destLinkTable + " SET EDGE_MODE=0");
      connector.getConnection().commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    createRIndex(destLinkTable, destLinkTable + "_SIDX", "GEOMETRY", dim1, dim2, srid);
    int nextLinkId = -1;
    try {
      ResultSet resultSet = connector.getStatement().executeQuery("SELECT MAX(ID) FROM " + destLinkTable);
      if (resultSet.next()) {
        nextLinkId = resultSet.getInt(1);
        return nextLinkId;
      } else {
        throw new RuntimeException("Problems when reading max id value");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return Integer.MAX_VALUE;

  }

  private void createRIndex(String tableName, String indexName, String geometry, String dim1, String dim2, String srid) {
    StringBuilder b = new StringBuilder();
    b.append("DELETE mdsys.user_sdo_geom_metadata WHERE TABLE_NAME=UPPER('");
    b.append(tableName).append("')");
    try {
      connector.getStatement().execute(b.toString());
    } catch (SQLException e) {
      // e.printStackTrace();
    }

    b.delete(0, b.length());
    b.append("INSERT INTO mdsys.user_sdo_geom_metadata VALUES ('");
    b.append(tableName).append("','").append(geometry).append("',");
    b.append("MDSYS.SDO_DIM_ARRAY(");
    b.append(dim1);
    b.append(",");
    b.append(dim2);
    b.append("),");
    b.append(srid);
    b.append(")");

    try {
      connector.getStatement().execute(b.toString());
    } catch (SQLException e) {
      // e.printStackTrace();
    }
    b.delete(0, b.length());
    b.append("CREATE INDEX ").append(indexName);
    b.append(" ON ").append(tableName);
    b.append("(").append(geometry).append(")");
    b.append(" INDEXTYPE IS MDSYS.SPATIAL_INDEX");
    try {
      // connector.getStatment().execute("DROP INDEX " + indexName);
      connector.getStatement().execute(b.toString());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private int splitLinks(String srcLinkTable, String destLinkTable, String mappingTable, int nextLinkId) {
    System.out.println("Start: Splitting links and merging phase");

    SortedMap<Integer, LinkCluster> linkClusters = new TreeMap<Integer, LinkCluster>();
    Statement stmt = connector.getStatement();
    ResultSet rSet = null;
    StringBuilder b = new StringBuilder();
    b.append("SELECT M.BUS_NODE_ID, M.START_OFFSET, L.* FROM ");
    b.append(mappingTable).append(" M, ").append(srcLinkTable).append(" L");
    b.append(" WHERE M.PED_LINK_ID=L.ID AND M.DISTANCE_TO_LINE<250 ");
    b.append("ORDER BY L.ID, M.START_OFFSET");

    LinkCluster currentCluster = null;
    try {
      rSet = stmt.executeQuery(b.toString());
      while (rSet.next()) {
        int linkId = rSet.getInt("ID");
        int busNodeId = rSet.getInt("BUS_NODE_ID");
        double offset = rSet.getInt("START_OFFSET");
        double length = rSet.getInt("LENGTH");
        if (currentCluster != null && currentCluster.getLink().getId() == linkId) {
          currentCluster.addMapping(busNodeId, offset);
        } else {
          if (currentCluster != null) { // we add it to the cluster collection
            linkClusters.put(currentCluster.getLink().getId(), currentCluster);
          }
          int startNodeId = rSet.getInt("SOURCE");
          int endNodeId = rSet.getInt("TARGET");

          // double speed = rSet.getInt("SPEED");
          currentCluster = new LinkCluster(new Link(linkId, startNodeId, endNodeId, length));
          currentCluster.addMapping(busNodeId, offset);
        }
      }
      linkClusters.put(currentCluster.getLink().getId(), currentCluster);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rSet != null)
          rSet.close();
        if (stmt != null)
          stmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    SortedSet<Link> newLinks = new TreeSet<Link>();
    Set<NodeEntry> updatingNodes = new HashSet<NodeEntry>();
    // setting the offsets
    // iterate over each link
    for (Entry<Integer, LinkCluster> linkCluster : linkClusters.entrySet()) {
      Link originLink = linkCluster.getValue().getLink();
      int idx = 0;
      // iterate over each offset
      OffsetEntry previousOffsetEntry = null;
      SortedMap<Double, OffsetEntry> mappingEntries = linkCluster.getValue().getMappingEntries();
      double minOffset = mappingEntries.firstKey();
      double maxOffset = mappingEntries.lastKey();
      if (minOffset == 0) {
        Set<Integer> busNodeIds = mappingEntries.get(minOffset).getAllBusNodeIds();
        if (busNodeIds.size() > 1) {
          StringBuffer buf = new StringBuffer();
          buf.append(" busnodes: ");
          for (Integer id : busNodeIds) {
            buf.append(id).append(",");
          }
          System.err.println("On edge:" + originLink.getId() + " there lay more than on bus nodes on the offset:"
              + minOffset + buf.toString());
        }
        // we only take the first one
        int busnode = busNodeIds.iterator().next();
        updatingNodes.add(new NodeEntry(originLink.getStartNodeId(), busnode, true));
        originLink.setStartNodeId(busnode);
        mappingEntries.remove(minOffset);

      }
      if (maxOffset > 0 && maxOffset == originLink.getLength()) {
        Set<Integer> busNodeIds = mappingEntries.get(maxOffset).getAllBusNodeIds();
        if (busNodeIds.size() > 1) {
          StringBuffer buf = new StringBuffer();
          buf.append(" busnodes: ");
          for (Integer id : busNodeIds) {
            buf.append(id).append(",");
          }
          System.err.println("On edge:" + originLink.getId() + " there lay more than on bus nodes on the offset:"
              + maxOffset + buf.toString());

        }
        // we only take the first one
        int busnode = busNodeIds.iterator().next();
        updatingNodes.add(new NodeEntry(originLink.getEndNodeId(), busnode, false));
        originLink.setEndNodeId(busnode);
        mappingEntries.remove(maxOffset);
      }

      for (Entry<Double, OffsetEntry> offsetEntry : mappingEntries.entrySet()) {
        int startNodeId;
        // double offset = offsetEntry.getKey();
        if (idx == 0) { // first one special handling
          startNodeId = originLink.getStartNodeId();
          for (Integer busNodeId : offsetEntry.getValue().getAllBusNodeIds()) {
            newLinks.add(new Link(nextLinkId++, startNodeId, busNodeId, 0.0, offsetEntry.getKey(), originLink.getId(),
                                  originLink.getLength()));
          }
        } else {
          // we connect every
          for (Integer previousBusNodeId : previousOffsetEntry.getAllBusNodeIds()) {
            for (Integer busNodeId : offsetEntry.getValue().getAllBusNodeIds()) {
              newLinks.add(new Link(nextLinkId++, previousBusNodeId, busNodeId, previousOffsetEntry.getOffset(),
                                    offsetEntry.getKey(), originLink.getId(), originLink.getLength()));
            }
          }
        }
        previousOffsetEntry = offsetEntry.getValue();
        idx++;
      }
      if (previousOffsetEntry != null) {
        // connect the end node
        for (Integer previousBusNodeId : previousOffsetEntry.getAllBusNodeIds()) {
          newLinks.add(new Link(nextLinkId++, previousBusNodeId, originLink.getEndNodeId(), previousOffsetEntry
              .getOffset(), originLink.getLength(), originLink.getId(), originLink.getLength()));
        }
      }
    }

    // drop from destination table the link
    String dropString = "DELETE FROM " + destLinkTable + " WHERE ID=:1";
    String updateStartNodesString = "UPDATE " + destLinkTable + " SET SOURCE=:1 WHERE SOURCE=:2";
    String updateEndNodesString = "UPDATE " + destLinkTable + " SET TARGET=:1 WHERE TARGET=:2";

    String insertString = "INSERT INTO " + destLinkTable
        + " (ID, SOURCE, TARGET, LENGTH, GEOMETRY,EDGE_MODE) VALUES (:1, :2, :3, :4 ,";
    insertString += " (SELECT SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.CLIP_GEOM_SEGMENT(SDO_LRS.CONVERT_TO_LRS_GEOM(GEOMETRY), :5, :6)) FROM ";
    insertString += srcLinkTable + " WHERE ID=:7),0)";

    PreparedStatement deleteStmt = null, insertStmt = null, updateSNodeStmt, updateEndNodeStmt;
    Set<Integer> linkIdsToDelete = new HashSet<Integer>();
    int insertedLinkCounter = 0, deletedLinkCounter = 0;

    int numOfInserts = 100;

    try {
      deleteStmt = connector.getConnection().prepareStatement(dropString);
      insertStmt = connector.getConnection().prepareStatement(insertString);
      updateSNodeStmt = connector.getConnection().prepareStatement(updateStartNodesString);
      updateEndNodeStmt = connector.getConnection().prepareStatement(updateEndNodesString);

      ((OraclePreparedStatement) deleteStmt).setExecuteBatch(numOfInserts);
      ((OraclePreparedStatement) insertStmt).setExecuteBatch(numOfInserts);
      ((OraclePreparedStatement) updateSNodeStmt).setExecuteBatch(numOfInserts);
      ((OraclePreparedStatement) updateEndNodeStmt).setExecuteBatch(numOfInserts);

      for (Link link : newLinks) {
        if (!linkIdsToDelete.contains(link.getOriginId())) {
          linkIdsToDelete.add(link.getOriginId());
          deleteStmt.setInt(1, link.getOriginId());
          // deleteStmt.addBatch();
          deletedLinkCounter++;
          if (deleteStmt.executeUpdate() == numOfInserts) {
            connector.getConnection().commit();
          }
          /*
           * if(deletedLinkCounter%100>0) { deleteStmt.executeBatch(); }
           */

        }
        insertStmt.setInt(1, link.getId());
        insertStmt.setInt(2, link.getStartNodeId());
        insertStmt.setInt(3, link.getEndNodeId());
        insertStmt.setDouble(4, link.getLength());
        insertStmt.setDouble(5, link.getStartOffset());
        insertStmt.setDouble(6, link.getEndOffset());
        insertStmt.setInt(7, link.getOriginId());
        // insertStmt.addBatch();
        insertedLinkCounter++;
        if (insertStmt.executeUpdate() == numOfInserts) {
          connector.getConnection().commit();
        }
        /*
         * if(insertedLinkCounter%100>0) { insertStmt.executeBatch(); }
         */
        //
      }

      for (NodeEntry node : updatingNodes) {
        if (node.isStartNode) {
          updateSNodeStmt.setInt(1, node.getNewNodeId());
          updateSNodeStmt.setInt(2, node.getOrigNodeId());
          if (updateSNodeStmt.executeUpdate() == numOfInserts) {
            connector.getConnection().commit();
          }
        } else {
          updateEndNodeStmt.setInt(1, node.getNewNodeId());
          updateEndNodeStmt.setInt(2, node.getOrigNodeId());
          if (updateEndNodeStmt.executeUpdate() == numOfInserts) {
            connector.getConnection().commit();
          }
        }
      }

      connector.getConnection().commit();

      // deleteStmt.executeBatch();
      // insertStmt.executeBatch();
      System.out.println("The number of deleted links:" + linkIdsToDelete.size());
      System.out.println("The number of inserted links:" + insertedLinkCounter);
      // System.out.println("The number of updatedinserted links:" + insertedLinkCounter);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (deleteStmt != null) {
          deleteStmt.close();
        }
        if (insertStmt != null) {
          insertStmt.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    System.out.println("End: Splitting links and merging phase");
    return nextLinkId;
  }

  public int getNextValue(String sequence) {
    int nextVal = -1;
    PreparedStatement pStmt = null;
    ResultSet rSet = null;

    try {
      pStmt = connector.getConnection().prepareStatement("SELECT " + sequence + ".NEXTVAL from dual");
      rSet = pStmt.executeQuery();
      if (rSet.next()) {
        nextVal = rSet.getInt(1);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (rSet != null) {
        try {
          rSet.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      if (pStmt != null) {
        try {
          pStmt.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    return nextVal;
  }

  public class NodeEntry {
    int origNodeId, newNodeId;
    boolean isStartNode;

    public NodeEntry(int origNodeId, int newNodeId, boolean isStartNode) {
      super();
      this.origNodeId = origNodeId;
      this.newNodeId = newNodeId;
      this.isStartNode = isStartNode;
    }

    public int getNewNodeId() {
      return newNodeId;
    }

    public int getOrigNodeId() {
      return origNodeId;
    }

    public boolean isStartNode() {
      return isStartNode;
    }

  }

  public static void main(String[] args) {
    String username = "mainnerebner", password = "mainnerebner1234", url = "maps.inf.unibz.it", sid = "maps", port = "1521";
    String pedLinkTable = "BZ_PED_LINKS", destLinkTable = "BZ_EDGES", destNodeTable = "BZ_NODES";
    String mappingTable = "BZ_MAPPING", busLinkTable = "BZ_BUS_LINKS", tripTable = "SASA_TRIPS";
    String dimension1Element = "MDSYS.SDO_DIM_ELEMENT('X',676836.212695921,680931.673598252,0.001)";
    String dimension2Element = "MDSYS.SDO_DIM_ELEMENT('Y', 5148549.05591258,5152597.63201377,0.001)";
    String srid = "82344";
    DBVendor database = DBVendor.ORACLE;

    // parsing input parameters
    for (String arg : args) {
      String value = arg.substring(arg.indexOf("=") + 1);
      if (arg.startsWith("database")) {
        database = DBVendor.valueOf(value.trim().toUpperCase());
      } else if (arg.startsWith("username")) {
        username = value.trim();
      } else if (arg.startsWith("password")) {
        password = value.trim();
      } else if (arg.startsWith("url")) {
        url = value.trim();
      } else if (arg.startsWith("sid")) {
        sid = value.trim();
      } else if (arg.startsWith("port")) {
        port = value.trim();
      } else if (arg.startsWith("pedLinkTable")) {
        pedLinkTable = value.trim();
      } else if (arg.startsWith("destLinkTable")) {
        destLinkTable = value.trim();
      } else if (arg.startsWith("destNodeTable")) {
        destNodeTable = value.trim();
      } else if (arg.startsWith("mappingTable")) {
        mappingTable = value.trim();
      } else if (arg.startsWith("busLinkTable")) {
        busLinkTable = value.trim();
      } else if (arg.startsWith("tripTable")) {
        tripTable = value.trim();
      } else if (arg.startsWith("dimension1Element")) {
        dimension1Element = value.trim();
      } else if (arg.startsWith("dimension2Element")) {
        dimension2Element = value.trim();
      } else if (arg.startsWith("srid")) {
        srid = value.trim();
      }
    }

    MergedNetworkGenerator generator = new MergedNetworkGenerator(database, username, password, url, sid, port);
    // copying tables
    int maxLinkId = generator.copyTables(pedLinkTable, destLinkTable, dimension1Element, dimension2Element, srid);
    // splitting links
    maxLinkId = generator.splitLinks(pedLinkTable, destLinkTable, mappingTable, maxLinkId + 1);

    String linkSequence = "TMP_LINK_SEQUENCE";
    generator.createSequenceStartingWithMaxValues(linkSequence, destLinkTable, "ID");
    // dropTable(mappingTable);
    generator.createInvertedLinks(destLinkTable, maxLinkId, linkSequence);
    // String busLinkTable = createBusLinkTable(busScheduleTable, busTrips);
    generator.mergeLinks(destLinkTable, busLinkTable, tripTable, linkSequence);
    generator.createNodeTable(destNodeTable, destLinkTable);
    System.out.println("The tool has finished the generation successful.");
    System.out.println("Following tables were created:");
    System.out.println("Merged link table: " + destLinkTable);
    System.out.println("Merged node table: " + destNodeTable);

  }

}
