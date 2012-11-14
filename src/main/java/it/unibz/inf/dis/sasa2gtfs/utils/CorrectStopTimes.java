/**
 * 
 */
package it.unibz.inf.dis.sasa2gtfs.utils;

import it.unibz.inf.dis.db.DBVendor;
import it.unibz.inf.dis.db.JDBCConnector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * <p>
 * The <code>CorrectStopTimes</code> class
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
public class CorrectStopTimes {

  private JDBCConnector connector;
  private Map<Integer, Trip> trips = new HashMap<Integer, Trip>();

  public CorrectStopTimes(DBVendor database,String uname, String passw, String host, String sid, String port) {
      connector = new JDBCConnector(database,uname, passw, host, sid, port);
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
  public void correctTimeValues(String tableName, String tripIdColumn, String nodeIdColumn, String stopSeqColumn, String timeColumn) throws SQLException {
	    StringBuilder b = new StringBuilder();
	    b.append("SELECT ").append(tripIdColumn).append(",");
	    b.append(nodeIdColumn).append(",");
	    b.append(stopSeqColumn).append(",");
	    b.append(timeColumn).append(" FROM ");
	    b.append(tableName).append(" ORDER BY ");
	    b.append(tripIdColumn).append(",").append(stopSeqColumn);
	    
	    ResultSet rSet = connector.getStatement().executeQuery(b.toString());
	    while (rSet.next()) {
	      int bezID = rSet.getInt(tripIdColumn);
	      Trip trip;
	      if(!trips.containsKey(bezID)) {
	        trips.put(bezID, new Trip(bezID));
	      }
	      trip = trips.get(bezID);
	      int stopID = rSet.getInt(nodeIdColumn);
	      int stopSeq = rSet.getInt(stopSeqColumn);
	      Timestamp timestamp = rSet.getTimestamp(timeColumn);
	      trip.addEntry(new TripEntry(stopSeq,stopID,timestamp));
	    }
	    rSet.close();
	    
	    
	    List<Trip> tripsToUpdate = new ArrayList<Trip>();
	    
	    
	    for (Trip trip : trips.values()) {
	      TripEntry first=trip.first(), last=trip.last();
	      if(first.getTimestamp()==null){
	        int i=0;
	        Iterator<TripEntry> it = trip.getEntries().iterator();
	        while(it.hasNext()){
	          if(i==1){
	            first = it.next();
	            break;
	          }
	          it.next();
	          i++;
	        }
	      }
	      if(last.getTimestamp()==null){
	        int i=0;
	        Iterator<TripEntry> it = trip.getEntries().iterator();
	        while(it.hasNext()){
	          if(i==trip.size()-2){
	            last = it.next();
	            break;
	          }
	          it.next();
	          i++;
	        }
	      }
	      if(first.getTimestamp().after(last.getTimestamp())){
	        TripEntry previousEntry=null;
	        for (TripEntry entry : trip.entries) {
	          if(entry.getTimestamp()==null){
	            continue;
	          } 
	          if(previousEntry!=null) {
	            if(previousEntry.getTimestamp().after(entry.getTimestamp())){
	              Calendar calendar = Calendar.getInstance();
	              calendar.setTimeInMillis(entry.getTimestamp().getTime());
	              calendar.add(Calendar.DAY_OF_MONTH, 1);
	              entry.getTimestamp().setTime(calendar.getTimeInMillis());
	              entry.setDirty(true);
	            }
	          }
	          previousEntry = entry;
	        }
	        tripsToUpdate.add(trip);
	      }
	    }
	    
	    b = new StringBuilder();
	    b.append("UPDATE ").append(tableName).append(" SET ");
	    b.append(timeColumn).append("=:1 WHERE ");
	    b.append(tripIdColumn).append("=:2 ");
	    b.append("AND ").append(nodeIdColumn).append("=:3 ");
	    b.append("AND ").append(stopSeqColumn).append("=:4 ");
	    PreparedStatement prepareStatement = connector.getConnection().prepareStatement(b.toString());
	    int rowCntr=0;
	    
	    for (Trip trip : tripsToUpdate) {
	      for (TripEntry entry : trip.getEntries()) {
	        if(entry.isDirty()) {
	          prepareStatement.setTimestamp(1, entry.getTimestamp());
	          prepareStatement.setInt(2, trip.getTripId());
	          prepareStatement.setInt(3, entry.getStopId());
	          prepareStatement.setInt(4, entry.getStopSequence());
	          prepareStatement.addBatch();
	          if(rowCntr%1000==0){
	            prepareStatement.executeBatch();
	          }
	          rowCntr++;
	        }
	      }
	    }
	    prepareStatement.executeBatch();
	    connector.getConnection().commit();
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
    String username = "iso_prod";
    String password = "iso_prod";
    String url = "bz10m.inf.unibz.it";
    String sid = "bz10m";
    String port = "1521";
    String tableSchedule = "bz_STOPTIMES";
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
      } else if (arg.startsWith("tableSchedule")) {
        tableSchedule = value.trim();
      } 
    }

    try {
      CorrectStopTimes app = new CorrectStopTimes(database,username, password, url, sid, port);
      app.correctTimeValues(tableSchedule,"TRIP_ID","STOP_ID","STOP_SEQUENCE","DEPARTURE_TIME");
      //app.correctTimeValues(tableSchedule,"TRIP_ID","STOP_ID","STOP_SEQUENCE","ARRIVAL_TIME");
    } catch (SQLException e) {
      e.printStackTrace();

    }
  }
}


