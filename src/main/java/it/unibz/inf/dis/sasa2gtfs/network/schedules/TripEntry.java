package it.unibz.inf.dis.sasa2gtfs.network.schedules;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TripEntry {
  
  String stopName;
  Timestamp tArrival, tDeparture;
  int stopId = Integer.MIN_VALUE;
  DateFormat df = new SimpleDateFormat("HH'.'mm'.'ss"); 
  
  public TripEntry(String stopName, String arrivalTime, String departureTime) {
    this.stopName = stopName;
    try {
      tArrival = new Timestamp(df.parse(arrivalTime.trim()).getTime());
    } catch (ParseException e) {
      //e.printStackTrace();
    }
    try {
      //System.out.println(departureTime);
      tDeparture = new Timestamp(df.parse(departureTime.trim()).getTime());
    } catch (ParseException e) {
      //e.printStackTrace();
    }
  }

  public String getStopName() {
    return stopName;
  }

  public Timestamp getArrivalTime() {
    return tArrival;
  }

  public Timestamp getDepartureTime() {
    return tDeparture;
  }
  
  public int getStopId() {
    return stopId;
  }

  public void setStopId(int stopId) {
    this.stopId = stopId;
  }

}
