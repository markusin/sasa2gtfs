package it.unibz.inf.dis.sasa2gtfs.utils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class WeekTracker {

  private int weekDay;
  private List<Integer> enabledWeeks;
  private List<Integer> disabledWeeks;
  private SortedMap<String, Integer> patterns;
  private boolean enabeled = false;

  public WeekTracker(int weekDay, int weeks) {
    this.weekDay = weekDay;
    enabledWeeks = new ArrayList<Integer>(weeks);
    disabledWeeks = new ArrayList<Integer>(weeks);
    patterns = new TreeMap<String, Integer>();
  }

  void add(int week, int enabled) {
    if (enabled == 1) {
      enabledWeeks.add(week);
    } else {
      disabledWeeks.add(week);
    }
  }

  boolean isEnabled() {
    return enabeled;
  }

  void setEnabled(boolean enabled) {
    this.enabeled = enabled;
  }

  public List<Integer> getDisabledWeeks() {
    return disabledWeeks;
  }

  public List<Integer> getEnabledWeeks() {
    return enabledWeeks;
  }

  public int getWeekDay() {
    return weekDay;
  }
  
  public String getMostFrequenPattern() {
    String firstKey = patterns.firstKey();
    String lastKey = patterns.lastKey();
    return firstKey;
  }

  @Override
  public String toString() {
    
    String day = "";
    if (weekDay == Calendar.MONDAY-1)
      day = "Monday";
    else if (weekDay == Calendar.TUESDAY-1)
      day = "Tuesday";
    else if (weekDay == Calendar.WEDNESDAY-1)
      day = "Wednesday";
    else if (weekDay == Calendar.THURSDAY-1)
      day = "Thursday";
    else if (weekDay == Calendar.FRIDAY-1)
      day = "Friday";
    else if (weekDay == Calendar.SATURDAY-1)
      day = "Saturday";
    else if (weekDay == Calendar.SUNDAY-1)
      day = "Sunday";

    return day + ":" + (isEnabled() ? "1" : "0");
  }

}
