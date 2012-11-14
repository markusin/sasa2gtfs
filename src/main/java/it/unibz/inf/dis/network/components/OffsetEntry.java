/**
 * 
 */
package it.unibz.inf.dis.network.components;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * <p>The <code>OffsetEntry</code> class</p>
 * <p>Copyright: 2006 - 2009 <a href="http://www.inf.unibz.it/dis">Dis Research Group</a></p>
 * <p> Domenikanerplatz -  Bozen, Italy.</p>
 * <p> </p>
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 2.2
 */
public class OffsetEntry {
  
  SortedSet<Integer> busNodeIds = new TreeSet<Integer>();
  double offset;
  
  
  public OffsetEntry(double offset) {
    this.offset = offset;
  }
  
  public void addBusNodeId(int busNodeId){
    busNodeIds.add(busNodeId);
  }
  
  public Set<Integer> getAllBusNodeIds() {
    return busNodeIds;
  }
  
  public double getOffset() {
    return offset;
  }
  
  
}
