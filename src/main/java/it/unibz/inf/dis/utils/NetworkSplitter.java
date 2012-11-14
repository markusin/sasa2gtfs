package it.unibz.inf.dis.utils;

import it.unibz.inf.dis.db.DBVendor;
import it.unibz.inf.dis.db.JDBCConnector;
import it.unibz.inf.dis.network.components.Link;
import it.unibz.inf.dis.network.components.LinkCluster;
import it.unibz.inf.dis.network.components.OffsetEntry;

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

public class NetworkSplitter {

  private JDBCConnector connector;

  public NetworkSplitter(DBVendor db, String username, String password, String url, String sid, String port) {
    connector = new JDBCConnector(db, username, password, url, sid, port);
  }

  /**
   * <p>
   * Method splitLinks
   * </p>
   * 
   * @param pedEdgeTable
   * @param edgeTable
   * @param mappingTable
   * @return
   */
  private void splitLinks(String pedEdgeTable, String edgeTable, String mappingTable) {
    System.out.println("Start: Splitting links and merging phase");

    SortedMap<Integer, LinkCluster> linkClusters = new TreeMap<Integer, LinkCluster>();
    Statement stmt = null;
    ResultSet rSet = null;

    int maxLinkId = Integer.MIN_VALUE;
    try {
      stmt = connector.getStatement();
      rSet = stmt.executeQuery("SELECT MAX(ID) FROM " + pedEdgeTable);
      if (rSet.next()) {
        maxLinkId = rSet.getInt(1) + 1;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rSet != null)
          rSet.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    StringBuilder b = new StringBuilder();
    b.append("SELECT L.ID,M.STOP_ID,M.START_OFFSET,L.LENGTH,L.SOURCE,L.TARGET FROM ");
    b.append(mappingTable).append(" M, ").append(pedEdgeTable).append(" L");
    b.append(" WHERE M.EDGE_ID=L.ID AND M.DISTANCE_TO_LINE<200 ");
    b.append("ORDER BY L.ID, M.START_OFFSET");

    LinkCluster currentCluster = null;
    ResultSet rSet2 = null;
    try {
      stmt = connector.getStatement();
      rSet2 = stmt.executeQuery(b.toString());
      while (rSet2.next()) {
        int linkId = rSet2.getInt("ID");
        int busNodeId = rSet2.getInt("STOP_ID");
        double offset = rSet2.getInt("START_OFFSET");
        double length = rSet2.getInt("LENGTH");
        if (currentCluster != null && currentCluster.getLink().getId() == linkId) {
          currentCluster.addMapping(busNodeId, offset);
        } else {
          if (currentCluster != null) { // we add it to the cluster collection
            linkClusters.put(currentCluster.getLink().getId(), currentCluster);
          }
          int startNodeId = rSet2.getInt("SOURCE");
          int endNodeId = rSet2.getInt("TARGET");
          currentCluster = new LinkCluster(new Link(linkId, startNodeId, endNodeId, length));
          currentCluster.addMapping(busNodeId, offset);
        }
      }
      linkClusters.put(currentCluster.getLink().getId(), currentCluster);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rSet2 != null)
          rSet2.close();
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
            newLinks.add(new Link(maxLinkId++, startNodeId, busNodeId, 0.0, offsetEntry.getKey(), originLink.getId(),
                                  originLink.getLength()));
          }
        } else {
          // we connect every
          for (Integer previousBusNodeId : previousOffsetEntry.getAllBusNodeIds()) {
            for (Integer busNodeId : offsetEntry.getValue().getAllBusNodeIds()) {
              newLinks.add(new Link(maxLinkId++, previousBusNodeId, busNodeId, previousOffsetEntry.getOffset(),
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
          newLinks.add(new Link(maxLinkId++, previousBusNodeId, originLink.getEndNodeId(), previousOffsetEntry
              .getOffset(), originLink.getLength(), originLink.getId(), originLink.getLength()));
        }
      }
    }

    // drop from destination table the link
    String dropString = "DELETE FROM " + edgeTable + " WHERE ID=:1";
    String updateStartNodesString = "UPDATE " + edgeTable + " SET SOURCE=:1 WHERE SOURCE=:2";
    String updateEndNodesString = "UPDATE " + edgeTable + " SET TARGET=:1 WHERE TARGET=:2";

    String insertString = "INSERT INTO " + edgeTable
        + " (ID, SOURCE, TARGET, LENGTH, GEOMETRY,EDGE_MODE) VALUES (:1, :2, :3, :4 ,";
    insertString += " (SELECT SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.CLIP_GEOM_SEGMENT(SDO_LRS.CONVERT_TO_LRS_GEOM(GEOMETRY), :5, :6)) FROM ";
    insertString += pedEdgeTable + " WHERE ID=:7),0)";

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
      connector.getConnection().commit();

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
  }

  /**
   * <p>
   * The <code>NodeEntry</code> class
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
    String username = "iso_dev", password = "iso_dev", url = "bz10m.inf.unibz.it", sid = "bz10m", port = "1521";
    String pedEdgeTable = "IT_PED_EDGES", edgeTable = "IT_EDGES", mappingTable = "IT_MAPPING";
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
      } else if (arg.startsWith("pedEdgeTable")) {
        pedEdgeTable = value.trim();
      } else if (arg.startsWith("edgeTable")) {
        edgeTable = value.trim();
      } else if (arg.startsWith("mappingTable")) {
        mappingTable = value.trim();
      }
    }
    NetworkSplitter generator = new NetworkSplitter(database, username, password, url, sid, port);
    generator.splitLinks(pedEdgeTable, edgeTable, mappingTable);
  }
}
