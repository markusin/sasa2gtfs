package it.unibz.inf.dis.sasa2gtfs.network.schedules;

import java.sql.Timestamp;

public class ScheduleEntry {

  int stopSequence, stopId;
  Timestamp tArrival, tDeparture;

  public ScheduleEntry(int stopId, int stopSequence, Timestamp tArrival, Timestamp tDeparture) {
    this.stopId = stopId;
    this.stopSequence = stopSequence;
    this.tArrival = tArrival;
    this.tDeparture = tDeparture;
  }

  public int getStopId() {
    return stopId;
  }

  public Timestamp getArrivalTime() {
    return tArrival;
  }

  public Timestamp getDepartureTime() {
    return tDeparture;
  }

  public int getStopSequence() {
    return stopSequence;
  }

}
