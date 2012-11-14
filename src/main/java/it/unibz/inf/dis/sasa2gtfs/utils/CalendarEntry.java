/**
 * 
 */
package it.unibz.inf.dis.sasa2gtfs.utils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 *
 * <p>The <code>CalendarEntry</code> class</p>
 * <p>Copyright: 2006 - 2009 <a href="http://www.inf.unibz.it/dis">Dis Research Group</a></p>
 * <p> Domenikanerplatz -  Bozen, Italy.</p>
 * <p> </p>
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 2.2
 */
public class CalendarEntry {

  private Calendar startDate;
  private Calendar endDate;
  private List<Calendar> disabledExceptions = new ArrayList<Calendar>();
  private List<Calendar> enabledExceptions = new ArrayList<Calendar>();
  private int[] days = { 0, 0, 0, 0, 0, 0, 0 };
  private String pattern;

  public CalendarEntry(Calendar startDate, Calendar endDate, WeekTracker[] trackers, String pattern) {
    this.startDate = startDate;
    this.endDate = endDate;
    for (int i = 0; i < trackers.length; i++) {
      WeekTracker tracker = trackers[i];
      if (tracker.isEnabled()) {
        days[tracker.getWeekDay()] = 1;
      }
      for (Integer disabledWeek : tracker.getDisabledWeeks()) {
        Calendar exception = (Calendar) startDate.clone();
        exception.add(Calendar.DAY_OF_YEAR, tracker.getWeekDay() * (disabledWeek + 1));
        disabledExceptions.add(exception);
      }
      for (Integer enabledWeek : tracker.getEnabledWeeks()) {
        Calendar exception = (Calendar) startDate.clone();
        exception.add(Calendar.DAY_OF_YEAR, tracker.getWeekDay() * (enabledWeek + 1));
        enabledExceptions.add(exception);
      }
    }
    this.pattern = pattern;
  }

  public Calendar getStartDate() {
    return startDate;
  }

  public void setStartDate(Calendar startDate) {
    this.startDate = startDate;
  }

  public Calendar getEndDate() {
    return endDate;
  }

  public void setEndDate(Calendar endDate) {
    this.endDate = endDate;
  }

  public List<Calendar> getDisabledExceptions() {
    return disabledExceptions;
  }

  public void setDisabledExceptions(List<Calendar> disabledExceptions) {
    this.disabledExceptions = disabledExceptions;
  }

  public List<Calendar> getEnabledExceptions() {
    return enabledExceptions;
  }

  public void setEnabledExceptions(List<Calendar> enabledExceptions) {
    this.enabledExceptions = enabledExceptions;
  }

  public int[] getDays() {
    return days;
  }

  public void setDays(int[] days) {
    this.days = days;
  }
  
  public String getPattern() {
    return pattern;
  }
  
}
