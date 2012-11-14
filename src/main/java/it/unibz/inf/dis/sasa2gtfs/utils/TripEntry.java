package it.unibz.inf.dis.sasa2gtfs.utils;

import java.sql.Timestamp;

public class TripEntry {
  boolean dirty;
  int seq, stopId;
  Timestamp timestamp;
  
  public TripEntry(int seq, int stopId, Timestamp timestamp) {
    this.seq = seq;
    this.stopId = stopId;
    this.timestamp = timestamp;
  }
  
  public int getStopId() {
    return stopId;
  }
  
  public Timestamp getTimestamp() {
    return timestamp;
  }
  
  public int getStopSequence() {
    return seq;
  }
  
  public boolean isDirty() {
    return dirty;
  }
  
  public void setDirty(boolean dirty) {
    this.dirty = dirty;
  }
  
}