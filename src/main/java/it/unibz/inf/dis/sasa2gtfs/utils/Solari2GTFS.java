/**
 * 
 */
package it.unibz.inf.dis.sasa2gtfs.utils;

import it.unibz.inf.dis.db.DBVendor;
import it.unibz.inf.dis.db.JDBCConnector;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * The <code>Solari2GTFS</code> class
 * </p>
 * <p>
 * Copyright: 2006 - 2009 <a href="http://www.inf.unibz.it/dis">Dis Research Group</a>
 * </p>
 * <p>
 * Domenikanerplatz - Bozen, Italy.
 * </p>
 * <p>
 * </p>
 * 
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 2.2
 */
public class Solari2GTFS {

  private JDBCConnector connector;
  private static final int ORA_ERROR_TABLE_NOT_EXIST = 942;

  public Solari2GTFS(DBVendor database,String uname, String passw, String host, String sid, String port) {
    // TODO Auto-generated constructor stub
      connector = new JDBCConnector(database,uname, passw, host, sid, port);
  }

  /**
   * <p>
   * Method migrateCalendarValues
   * </p>
   * migrates the data from the source table in Solari format into the destination table in GTFS format
   * 
   * @param sourceTable the name of the source table (with Solari format)
   * @param destCalendarTable the name of the destination calendar table (with GTFS format)
   * @param destCalendarExceptionTable the name of the destination calendar table (with GTFS format) dealing with
   *          exceptions
   * @throws SQLException
   */
  public void migrateCalendarValues(String sourceTable, String destCalendarTable, String destCalendarExceptionTable) {
    Set<CalendarEntry> calendarEntries = new HashSet<CalendarEntry>();

    truncateTables(sourceTable, destCalendarTable, destCalendarExceptionTable);

    ResultSet resultSet = null;
    try {
      resultSet = connector.getStatement().executeQuery(
          "SELECT KALENDER, MAX(GUELTIGKEIT_AB) FROM " + sourceTable + " GROUP BY KALENDER HAVING COUNT(*)>0");
      while (resultSet.next()) {
        String dayBits = resultSet.getString(1).trim();
        int firstOneIdx = dayBits.indexOf("1");
        int startingIndex = firstOneIdx - firstOneIdx % 7;
        int lastOneIdx = dayBits.lastIndexOf("1");
        int endingIndex = (lastOneIdx % 7 == 0) ? lastOneIdx : lastOneIdx + (7 - 166 % 7);

        // String substring = dayBits.substring(startingIndex, endingIndex);
        int weeks = (endingIndex - startingIndex) / 7;
        WeekTracker[] weekTrackers = new WeekTracker[7];
        int[] days = { 0, 0, 0, 0, 0, 0, 0 };

        Date date = resultSet.getDate(2);
        Calendar startDate = GregorianCalendar.getInstance();
        startDate.setTimeInMillis(date.getTime());
        int firstDay = startDate.get(Calendar.DAY_OF_WEEK) - 1;
        // System.out.println("First day:" + f.format(startDate.getTime()));
        startDate.add(Calendar.DAY_OF_MONTH, startingIndex);
        Calendar endDate = (Calendar) startDate.clone();
        endDate.add(Calendar.DAY_OF_YEAR, endingIndex);
        // System.out.println("First day:" + f.format(startDate.getTime()));
        // System.out.println("Last day:" + f.format(endDate.getTime()));

        // int firstWeekDay = startDate.get(Calendar.DAY_OF_WEEK) - 1; // we assume monday is the first day
        // we assume sunday represents the number 0, that is one val	ue less then the calendar day constant value
        firstDay = startDate.get(Calendar.DAY_OF_WEEK) - 1;

        for (int i = startingIndex; i <= endingIndex; i++) {
          int idx = (firstDay + i) % 7;
          if (i < startingIndex + 7) { // then filling the array
            WeekTracker tracker = new WeekTracker(idx, weeks);
            tracker.add(0, Integer.parseInt(String.valueOf(dayBits.charAt(i))));
            weekTrackers[idx] = tracker;

          } else {
            WeekTracker tracker = weekTrackers[idx];
            tracker.add((i - startingIndex + 1) / 7, Integer.parseInt(String.valueOf(dayBits.charAt(i))));
          }
        }
        for (int i = 0; i < weekTrackers.length; i++) {
          WeekTracker weekTracker = weekTrackers[i];
          weekTracker.setEnabled(weekTracker.getEnabledWeeks().size() >= weekTracker.getDisabledWeeks().size());
          if (weekTracker.isEnabled()) {
            days[weekTracker.getWeekDay()] = 1;
            weekTracker.getEnabledWeeks().clear();
          } else {
            days[weekTracker.getWeekDay()] = 0;
            weekTracker.getDisabledWeeks().clear();
          }
        }
        calendarEntries.add(new CalendarEntry(startDate, endDate, weekTrackers, dayBits));
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy'-'MM'-'dd");
    PreparedStatement insertCalendarDateStmt = null, updateTripStmt = null, insertCalendarStmt = null;

    try {
      insertCalendarDateStmt = connector.getConnection().prepareStatement(
          "INSERT INTO " + destCalendarExceptionTable + " (SERVICE_ID, DATUM, EXCEPTION_TYPE) VALUES(:1,:2,:3)");

      updateTripStmt = connector.getConnection().prepareStatement(
          "UPDATE " + sourceTable + " SET SERVICE_ID=:1 WHERE REPLACE(LTRIM(RTRIM(KALENDER)),CHR(13), NULL)=:2");

      resultSet = connector.getStatement().executeQuery("SELECT MAX(SERVICE_ID) FROM " + destCalendarTable);
      int serviceID = 0;
      if (resultSet.next()) {
        serviceID = resultSet.getInt(1) + 1;
      }
      // startDate.get(Calendar.DAY_OF_WEEK);
      String firstPart = "INSERT INTO " + destCalendarTable + "  (SERVICE_ID,START_DATE, END_DATE,";
      String lastPart = ", PATTERN) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)";

      for (CalendarEntry calendarEntry : calendarEntries) {
        String dayColumnString = "";
        switch (calendarEntry.getStartDate().get(Calendar.DAY_OF_WEEK)) {
          case Calendar.SUNDAY:
            dayColumnString = "SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY";
            break;
          case Calendar.MONDAY:
            dayColumnString = "MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY";
            break;
          case Calendar.TUESDAY:
            dayColumnString = "TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY, MONDAY";
            break;
          case Calendar.WEDNESDAY:
            dayColumnString = "WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY, MONDAY, TUESDAY";
            break;
          case Calendar.THURSDAY:
            dayColumnString = "THURSDAY, FRIDAY, SATURDAY, SUNDAY, MONDAY, TUESDAY, WEDNESDAY";
            break;
          case Calendar.FRIDAY:
            dayColumnString = "FRIDAY, SATURDAY, SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY";
            break;
          case Calendar.SATURDAY:
            dayColumnString = "SATURDAY, SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY";
            break;
          default:
            break;
        }
        if (!dayColumnString.isEmpty()) {
          insertCalendarStmt = connector.getConnection().prepareStatement(firstPart + dayColumnString + lastPart);
          insertCalendarStmt.setInt(1, serviceID);
          insertCalendarStmt.setDate(2, new Date(calendarEntry.getStartDate().getTimeInMillis()));
          insertCalendarStmt.setDate(3, new Date(calendarEntry.getEndDate().getTimeInMillis()));
          for (int i = 0; i < calendarEntry.getDays().length; i++) {
            insertCalendarStmt.setInt(4 + i, calendarEntry.getDays()[i]);
          }
          insertCalendarStmt.setString(11, calendarEntry.getPattern());
          insertCalendarStmt.executeUpdate();

          updateTripStmt.setInt(1, serviceID);
          updateTripStmt.setString(2, calendarEntry.getPattern());
          updateTripStmt.executeUpdate();

          for (Calendar disEx : calendarEntry.getDisabledExceptions()) {
            insertCalendarDateStmt.setInt(1, serviceID);
            insertCalendarDateStmt.setDate(2, new Date(disEx.getTimeInMillis()));
            insertCalendarDateStmt.setInt(3, 2);
            insertCalendarDateStmt.executeUpdate();
          }

          for (Calendar disEx : calendarEntry.getEnabledExceptions()) {
            insertCalendarDateStmt.setInt(1, serviceID);
            insertCalendarDateStmt.setDate(2, new Date(disEx.getTimeInMillis()));
            insertCalendarDateStmt.setInt(3, 1);
            insertCalendarDateStmt.executeUpdate();
          }
          serviceID++;
        }
      }
      connector.getConnection().commit();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (insertCalendarStmt != null) {
        try {
          if (!insertCalendarStmt.isClosed())
            insertCalendarStmt.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      if (insertCalendarDateStmt != null) {
        try {
          if (!insertCalendarDateStmt.isClosed())
            insertCalendarDateStmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (updateTripStmt != null) {
        try {
          if (!updateTripStmt.isClosed())
            updateTripStmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * <p>
   * Method truncateTables
   * </p>
   * 
   * @param destCalendarTable
   * @param destCalendarExceptionTable
   */
  private void truncateTables(String sourceTable, String destCalendarTable, String destCalendarExceptionTable) {
    Statement stmt = connector.getStatement();
    try {
      stmt.executeUpdate("TRUNCATE TABLE " + destCalendarTable);
      connector.getConnection().commit();
    } catch (SQLException e) {
      if (e.getErrorCode() == ORA_ERROR_TABLE_NOT_EXIST) {
        StringBuilder b = new StringBuilder();
        b.append("CREATE TABLE ").append(destCalendarTable).append("( ");
        b.append("SERVICE_ID NUMBER(5,0) NOT NULL ENABLE, ");
        b.append("START_DATE DATE NOT NULL ENABLE, ");
        b.append("END_DATE DATE NOT NULL ENABLE, ");
        b.append("MONDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("TUESDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("WEDNESDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("THURSDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("FRIDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("SATURDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("SUNDAY NUMBER(1,0) NOT NULL ENABLE, ");
        b.append("PATTERN VARCHAR2(400 BYTE), ");
        b.append("CONSTRAINT ").append(destCalendarTable).append("_PK  PRIMARY KEY(SERVICE_ID))");
        try {
          stmt.executeUpdate(b.toString());
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
    }
    try {
      stmt.executeUpdate("TRUNCATE TABLE " + destCalendarExceptionTable);
      connector.getConnection().commit();
    } catch (SQLException e) {
      if (e.getErrorCode() == ORA_ERROR_TABLE_NOT_EXIST) {
        StringBuilder b = new StringBuilder();
        b.append("CREATE TABLE ").append(destCalendarExceptionTable).append("( ");
        b.append("SERVICE_ID NUMBER(5,0) NOT NULL ENABLE, ");
        b.append("\"date\" DATE NOT NULL ENABLE, ");
        b.append("EXCEPTION_TYPE NUMBER(5,0) NOT NULL ENABLE, ");
        b.append("CONSTRAINT ").append(destCalendarExceptionTable).append("_PK PRIMARY KEY(SERVICE_ID))");
        try {
          stmt.executeUpdate(b.toString());
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
    }
    try {
      stmt.executeUpdate("ALTER TABLE " + sourceTable + " ADD (SERVICE_ID NUMBER(5,0))");
      connector.getConnection().commit();
    } catch (SQLException e) {
      // e.printStackTrace();
    }

    // TODO Auto-generated method stub

  }

  /**
   * <p>
   * Method main
   * </p>
   * 
   * @param args
   */
  public static void main(String[] args) {

    String username = "mainnerebner";
    String password = "mainnerebner1234";
    String url = "maps.inf.unibz.it";
    String sid = "maps";
    String port = "1521";
    String sourceTable = "TMP_SASA_TRIPS";
    String destCalendarTable = "BZ_CALENDAR";
    String destCalendarExceptionTable = "BZ_CALENDAR_DATES";
    DBVendor database = DBVendor.ORACLE;

    // parsing input parameters
    for (String arg : args) {
      String value = arg.substring(arg.indexOf("=") + 1);
      if (arg.startsWith("database")) {
        database = DBVendor.valueOf(value.trim().toUpperCase());
      } else  if (arg.startsWith("username")) {
        username = value.trim();
      } else if (arg.startsWith("password")) {
        password = value.trim();
      } else if (arg.startsWith("url")) {
        url = value.trim();
      } else if (arg.startsWith("sid")) {
        sid = value.trim();
      } else if (arg.startsWith("port")) {
        port = value.trim();
      } else if (arg.startsWith("sourceTable")) {
        sourceTable = value.trim();
      } else if (arg.startsWith("destCalendarTable")) {
        destCalendarTable = value.trim();
      } else if (arg.startsWith("destCalendarExceptionTable")) {
        destCalendarExceptionTable = value.trim();
      }
    }

    // System.out.println("Correcting Calendar to GTFS");
    Solari2GTFS app = new Solari2GTFS(database,username, password, url, sid, port);
    app.migrateCalendarValues(sourceTable, destCalendarTable, destCalendarExceptionTable);
    System.out.println("finished correcting Calendar");
  }
}
