/**
 * 
 */
package it.unibz.inf.dis.utils;


import it.unibz.inf.dis.db.DBVendor;
import it.unibz.inf.dis.db.JDBCConnector;
import it.unibz.inf.dis.sasa2gtfs.network.schedules.ScheduleEntry;
import it.unibz.inf.dis.sasa2gtfs.utils.Trip;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.jdbc.OraclePreparedStatement;

/**
 * <p>
 * The <code>CorrectOverFloatingStopTimes</code> class
 * </p>
 * the class correct values in the table stoptimes that have a date overflow, what means that their time value
 * (departure or arrivale) exceed midnight
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
public class CorrectOverFloatingStopTimes {

  private JDBCConnector connector;
  private Map<Integer, Trip> trips = new HashMap<Integer, Trip>();

  public CorrectOverFloatingStopTimes(DBVendor database, String uname, String passw, String host, String sid,
                                      String port) {
    connector = new JDBCConnector(database, uname, passw, host, sid, port);
  }

  /**
   * <p>
   * Method correctTimeValues
   * </p>
   * 
   * @param tableName the table of which to correct the values
   * @param tripIdColumn the column name of the the trip
   * @param nodeIdColumn the column name of the stop
   * @param stopSeqColumn the column name of the sequence number
   * @param timeColumn the column name of the time
   * @throws SQLException
   */
  public Map<Integer, Timestamp> findTripsToCorrect(String tableName) throws SQLException {
    Map<Integer, Timestamp> tripsToCorrect = new HashMap<Integer, Timestamp>();

    // choose the minimum from each latest departure time
    StringBuilder b = new StringBuilder();
    b.append("SELECT DISTINCT S.TRIP_ID,S.DEPARTURE_TIME FROM ");
    b.append("(SELECT S0.* FROM ");
    b.append(tableName).append(" S0,");
    b.append("(SELECT MIN(STOP_SEQUENCE) MIN_SS,TRIP_ID FROM ").append(tableName).append(" GROUP BY TRIP_ID) S1");
    b.append(" WHERE S0.TRIP_ID=S1.TRIP_ID AND S0.STOP_SEQUENCE=S1.MIN_SS ) S,");
    b.append(" (SELECT ARRIVAL_TIME,TRIP_ID FROM ").append(tableName).append(") S2 ");
    b.append(" WHERE S.DEPARTURE_TIME>S2.ARRIVAL_TIME AND S.TRIP_ID=S2.TRIP_ID");
    

    /*
      SELECT DISTINCT s.trip_id,s.departure_time FROM 
 (
  SELECT S0.* FROM 
    it_stoptimes S0,
   (SELECT MIN(STOP_SEQUENCE) MIN_SS,TRIP_ID FROM it_stoptimes GROUP BY trip_id) S1
   WHERE S0.TRIP_ID=S1.TRIP_ID AND S0.STOP_SEQUENCE=S1.MIN_SS
 ) S,
 (SELECT arrival_time,trip_id FROM it_stoptimes) s2
 WHERE s.departure_time>s2.arrival_time AND s.trip_id=s2.trip_id
     */

    ResultSet rSet = connector.getConnection().createStatement().executeQuery(b.toString());

    // ResultSet rSet = connector.getStatement().executeQuery();
    while (rSet.next()) {
      int tripId = rSet.getInt("TRIP_ID");
      Timestamp firstDepartureTime = rSet.getTimestamp("DEPARTURE_TIME");
      tripsToCorrect.put(tripId, firstDepartureTime);
    }
    rSet.close();
    return tripsToCorrect;
  }


  private void correctTrip(String tableName, Integer tripId, Timestamp minTime) {
    // TODO Auto-generated method stub
    StringBuilder b = new StringBuilder();
    b.append("SELECT STOP_ID,STOP_SEQUENCE,ARRIVAL_TIME,DEPARTURE_TIME FROM ");
    b.append(tableName).append(" WHERE TRIP_ID=:1 AND ARRIVAL_TIME<:2");

    List<ScheduleEntry> entries = new ArrayList<ScheduleEntry>();

    PreparedStatement prepareStatement = null;
    ResultSet rSet = null;
    try {
      prepareStatement = connector.getConnection().prepareStatement(b.toString());
      prepareStatement.setInt(1, tripId);
      prepareStatement.setTimestamp(2, minTime);
      rSet = prepareStatement.executeQuery();
      while (rSet.next()) {
        int stopId = rSet.getInt("STOP_ID");
        int stopSequence = rSet.getInt("STOP_SEQUENCE");
        Timestamp tArrival = rSet.getTimestamp("ARRIVAL_TIME");
        Timestamp tDeparture = rSet.getTimestamp("DEPARTURE_TIME");
        entries.add(new ScheduleEntry(stopId, stopSequence, tArrival, tDeparture));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rSet != null)
          rSet.close();
        if (prepareStatement != null)
          prepareStatement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    b = new StringBuilder();
    b.append("UPDATE ").append(tableName).append(" SET ARRIVAL_TIME=CAST(ARRIVAL_TIME+1 AS TIMESTAMP),");
    b.append(" DEPARTURE_TIME=CAST(DEPARTURE_TIME+1 AS TIMESTAMP) ");
    b.append(" WHERE TRIP_ID=:1 AND STOP_ID=:2 AND STOP_SEQUENCE=:3");
    
    
    StringBuilder b2 = new StringBuilder(); 
    b2.append("UPDATE ").append(tableName).append(" SET ARRIVAL_TIME=CAST(ARRIVAL_TIME+1 AS TIMESTAMP),");
    b2.append(" WHERE TRIP_ID=:1 AND STOP_ID=:2 AND STOP_SEQUENCE=:3");

    
    PreparedStatement prepareStatement2 = null;
    
    try {
      prepareStatement = connector.getConnection().prepareStatement(b.toString());
      prepareStatement2 = connector.getConnection().prepareStatement(b2.toString());
      ((OraclePreparedStatement) prepareStatement).setExecuteBatch(20);
      ((OraclePreparedStatement) prepareStatement2).setExecuteBatch(20);
      for (ScheduleEntry scheduleEntry : entries) {
        if (scheduleEntry.getArrivalTime()!=null) { 
          if(scheduleEntry.getArrivalTime().before(minTime)) {
            prepareStatement.setInt(1, tripId);
            prepareStatement.setInt(2, scheduleEntry.getStopId());
            prepareStatement.setInt(3, scheduleEntry.getStopSequence());
            if (prepareStatement.executeUpdate() == 20) {
              connector.getConnection().commit();
            }
          }
        } else {
          if(scheduleEntry.getArrivalTime().before(minTime)) {
            prepareStatement2.setInt(1, tripId);
            prepareStatement2.setInt(2, scheduleEntry.getStopId());
            prepareStatement2.setInt(3, scheduleEntry.getStopSequence());
            if (prepareStatement2.executeUpdate() == 20) {
              connector.getConnection().commit();
            }
          }
        }
      }
      connector.getConnection().commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (prepareStatement != null)
          prepareStatement.close();
        if (prepareStatement2 != null)
          prepareStatement2.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * <p>
   * Method main
   * </p>
   * 
   * @param args the arguments passed as property=value pair e.g. username=scott
   */
  public static void main(String[] args) {
    // default value setting
    String username = "iso_dev";
    String password = "iso_dev";
    String host = "bz10m.inf.unibz.it";
    String sid = "bz10m";
    String port = "1521";
    String stoptimesTableName = "IT_STOPTIMES";
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
        host = value.trim();
      } else if (arg.startsWith("sid")) {
        sid = value.trim();
      } else if (arg.startsWith("port")) {
        port = value.trim();
      } else if (arg.startsWith("stoptimesTableName")) {
        stoptimesTableName = value.trim();
      }
    }

    try {
      CorrectOverFloatingStopTimes app = new CorrectOverFloatingStopTimes(database, username, password, host, sid, port);
      Map<Integer, Timestamp> tripsToCorrect = app.findTripsToCorrect(stoptimesTableName);
      for (Integer tripId : tripsToCorrect.keySet()) {
        app.correctTrip(stoptimesTableName, tripId, tripsToCorrect.get(tripId));
      }
    } catch (SQLException e) {
      e.printStackTrace();

    }
  }
}
