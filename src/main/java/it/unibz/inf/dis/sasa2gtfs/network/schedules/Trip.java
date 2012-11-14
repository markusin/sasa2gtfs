package it.unibz.inf.dis.sasa2gtfs.network.schedules;

import java.util.ArrayList;
import java.util.List;

public class Trip {
  
  int id;
  List<TripEntry> tripEntries = new ArrayList<TripEntry>();
  
  
  public Trip(int id) {
    super();
    this.id = id;
  }
  
  public void addTripEntry(TripEntry trip){
    tripEntries.add(trip);
  }
  
  public int getId() {
    return id;
  }
  
  public List<TripEntry> getTripEntries() {
    return tripEntries;
  }

}
