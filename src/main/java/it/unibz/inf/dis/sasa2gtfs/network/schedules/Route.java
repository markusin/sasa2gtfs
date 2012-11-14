/**
 * 
 */
package it.unibz.inf.dis.sasa2gtfs.network.schedules;


/**
 *
 * <p>The <code>Route</code> class</p>
 * <p>Copyright: 2006 - 2009 <a href="http://www.inf.unibz.it/dis">Dis Research Group</a></p>
 * <p> Domenikanerplatz -  Bozen, Italy.</p>
 * <p> </p>
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 2.2
 */
public class Route {
  static int ID_COUNTER = 1;
  
  int id = Integer.MIN_VALUE;
  String shortName;
  String longName;
  String startStation;
  String terminateStation;
  
  Trip trip;
  
  public Route(String shortName, String longName) {
    this.shortName = shortName;
    this.longName = longName;
    this.id = ID_COUNTER++;
  }
  
  public void setTrip(Trip trip) {
    this.trip = trip;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getShortName() {
    return shortName;
  }

  public void setShortName(String shortName) {
    this.shortName = shortName;
  }

  public String getLongName() {
    return longName;
  }

  public void setLongName(String longName) {
    this.longName = longName;
  }

  public String getStartStation() {
    return startStation;
  }

  public void setStartStation(String startStation) {
    this.startStation = startStation;
  }

  public String getTerminateStation() {
    return terminateStation;
  }

  public void setTerminateStation(String terminateStation) {
    this.terminateStation = terminateStation;
  }

  public Trip getTrip() {
    return trip;
  }

}
