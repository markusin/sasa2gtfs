package it.unibz.inf.dis.network.components;


import java.util.SortedMap;
import java.util.TreeMap;


public class LinkCluster {
  
  Link link;
  /** An entry consisting of a pair of offset and a set of bus node ids */
  SortedMap<Double,OffsetEntry> mappings; 
  
  public LinkCluster(Link link) {
    this.link = link;
    mappings = new TreeMap<Double, OffsetEntry>();
  }
  
  public void addMapping(int nodeId, double offset) {
    OffsetEntry entry;
    if(mappings.containsKey(offset)){
      entry = mappings.get(offset); 
    } else {
      entry = new OffsetEntry(offset);
    }
    entry.addBusNodeId(nodeId);
    mappings.put(offset, entry);
  }
  
  public Link getLink() {
    return link;
  }
  
  public SortedMap<Double, OffsetEntry> getMappingEntries() {
    return mappings;
  }

}
