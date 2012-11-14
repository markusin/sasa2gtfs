package it.unibz.inf.dis.sasa2gtfs.utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

public class Trip {
  
  int tripId;
  TreeSet<TripEntry> entries = new TreeSet<TripEntry>(new Comparator<TripEntry>()  {
      public int compare(TripEntry o1, TripEntry o2) {
      if(o1.getStopSequence()<o2.getStopSequence()) return -1;
      if(o1.getStopSequence()>o2.getStopSequence()) return 1;
      return 0;
    }
    });
  TripEntry first, last;
  
  public Trip(int id) {
    this.tripId = id;
  }

  public int getTripId() {
    return tripId;
  }
  
  public void addEntry(TripEntry entry) {
    if(first==null) {
      first = entry;
    }
    last = entry;
    entries.add(entry);
  }
  
  public Collection<TripEntry> getEntries() {
    return entries;
  }
  
  public int size() {
    return entries.size();
  }
  
  public TripEntry first() {
    return entries.first();
  }
  
  public TripEntry last() {
    return entries.last();
  }

}
