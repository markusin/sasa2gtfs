/**
 * 
 */
package it.unibz.inf.dis.sasa2gtfs.utils;

import it.unibz.inf.dis.db.DBVendor;
import it.unibz.inf.dis.db.JDBCConnector;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import oracle.jdbc.OraclePreparedStatement;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

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
public class KMLImport {

  private JDBCConnector connector;

  private static final int ORA_ERROR_TABLE_NOT_EXIST = 942;

  public KMLImport(DBVendor database, String uname, String passw, String host, String sid, String port) {
    connector = new JDBCConnector(database, uname, passw, host, sid, port);
  }

  public void loadKMLFile(String urlKML, String tableName, String localFile) {
    truncateTable(tableName);
    System.out.println("Local file is: " + localFile);
    System.out.println("Table is: " + tableName);
    String s = "INSERT INTO " + tableName + "(STOP_ID,DESCRIPTION,LONGITUDE, LATITUDE,GEOMETRY) ("
        + "SELECT :1,:2,:3,:4, SDO_CS.TRANSFORM(SDO_GEOMETRY(:5,:6),:7) FROM DUAL)";

    Document document = null;
    try {
      // URL baseURL = new URL(urlKML);
      URL baseURL = Thread.currentThread().getContextClassLoader().getResource(localFile);
      //URL baseURL = ClassLoader.getSystemResource(localFile);
      
      InputStreamReader iReader = new InputStreamReader(baseURL.openStream(), Charset.defaultCharset());
      // File file = new File(baseURL.toExternalForm() + localFile);
      System.out.println("Local file is: " + baseURL.toExternalForm());
      try {
        document = new SAXBuilder().build(iReader);
      } catch (JDOMException e1) {
        e1.printStackTrace();
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    if (document != null) {
      PreparedStatement insertStmt;
      Namespace ns = Namespace.getNamespace("http://earth.google.com/kml/2.0");
      try {
        insertStmt = connector.getConnection().prepareStatement(s);
        ((OraclePreparedStatement) insertStmt).setExecuteBatch(300);
        @SuppressWarnings("unchecked")
        List<Element> placemarkers = document.getRootElement().getChild("Document", ns).getChildren("Placemark", ns);
        for (Element element : placemarkers) {
          String[] name = element.getChildText("name", ns).split(":");
          int stopId = Integer.valueOf(name[0].trim());
          String stopDescription = name[1].trim();
          String[] pointCoordindate = element.getChild("Point", ns).getChildText("coordinates", ns).split(",");
          double longitude = Double.valueOf(pointCoordindate[0].trim());
          double latitude = Double.valueOf(pointCoordindate[1].trim());
          String point = "POINT(" + longitude + " " + latitude + ")";
          insertStmt.setInt(1, stopId);
          insertStmt.setString(2, stopDescription);
          insertStmt.setDouble(3, longitude);
          insertStmt.setDouble(4, latitude);
          insertStmt.setString(5, point);
          insertStmt.setInt(6, 4326);
          insertStmt.setInt(7, 82344);
          if (insertStmt.executeUpdate() == 300) {
            connector.getConnection().commit();
          }
        }
        insertStmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * <p>
   * Method loadKMLFile
   * </p>
   * trys first to load the file from the remote kml and if not found it uses the local file
   * 
   * @param urlKML
   * @param tableName
   * @param localFile
   */
  public void loadKMLFile1(String urlKML, String tableName, String localFile) {
    truncateTable(tableName);
    URL url;

    System.out.println(localFile);
    String s = "INSERT INTO " + tableName + "(STOP_ID,DESCRIPTION,LONGITUDE, LATITUDE,GEOMETRY) ("
        + "SELECT :1,:2,:3,:4, SDO_CS.TRANSFORM(SDO_GEOMETRY(:5,:6),:7) FROM DUAL)";

    Document document = null;
    try {
      url = new URL(urlKML);
      InputStreamReader iReader = new InputStreamReader(url.openStream());
      document = new SAXBuilder().build(iReader);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
      File file = new File(localFile);
      try {
        document = new SAXBuilder().build(file);
      } catch (JDOMException e1) {
        e1.printStackTrace();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    } catch (JDOMException e) {
      e.printStackTrace();
    }

    if (document != null) {
      PreparedStatement insertStmt;
      Namespace ns = Namespace.getNamespace("http://earth.google.com/kml/2.0");
      try {
        insertStmt = connector.getConnection().prepareStatement(s);
        ((OraclePreparedStatement) insertStmt).setExecuteBatch(300);
        @SuppressWarnings("unchecked")
        List<Element> placemarkers = document.getRootElement().getChild("Document", ns).getChildren("Placemark", ns);
        for (Element element : placemarkers) {
          String[] name = element.getChildText("name", ns).split(":");
          int stopId = Integer.valueOf(name[0].trim());
          String stopDescription = name[1].trim();
          String[] pointCoordindate = element.getChild("Point", ns).getChildText("coordinates", ns).split(",");
          double longitude = Double.valueOf(pointCoordindate[0].trim());
          double latitude = Double.valueOf(pointCoordindate[1].trim());
          String point = "POINT(" + longitude + " " + latitude + ")";
          insertStmt.setInt(1, stopId);
          insertStmt.setString(2, stopDescription);
          insertStmt.setDouble(3, longitude);
          insertStmt.setDouble(4, latitude);
          insertStmt.setString(5, point);
          insertStmt.setInt(6, 4326);
          insertStmt.setInt(7, 82344);
          if (insertStmt.executeUpdate() == 300) {
            connector.getConnection().commit();
          }
        }
        connector.getConnection().commit();
        insertStmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * <p>
   * Method truncateTable
   * </p>
   * 
   * @param destCalendarTable
   * @param destCalendarExceptionTable
   */
  private void truncateTable(String tableName) {
    Statement stmt = connector.getStatement();
    try {
      stmt.executeUpdate("TRUNCATE TABLE " + tableName);
    } catch (SQLException e) {
      if (e.getErrorCode() == ORA_ERROR_TABLE_NOT_EXIST) {
        StringBuilder b = new StringBuilder();
        b.append("CREATE TABLE ").append(tableName).append(" ( ");
        b.append("STOP_ID NUMBER(10,0) NOT NULL ENABLE, ");
        b.append("DESCRIPTION VARCHAR2(512) NOT NULL ENABLE, ");
        b.append("LONGITUDE NUMBER(20,10) NOT NULL ENABLE, ");
        b.append("LATITUDE NUMBER(20,10) NOT NULL ENABLE, ");
        b.append("GEOMETRY SDO_GEOMETRY,");
        b.append("CONSTRAINT ").append(tableName).append("_PK  PRIMARY KEY(STOP_ID))");
        try {
          stmt.executeUpdate(b.toString());
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
    }
  }

  /**
   * <p>
   * Method main
   * </p>
   * 
   * @param args
   */
  public static void main(String[] args) {

    String username = "iso_dev";
    String password = "iso_dev";
    String url = "bz10m.inf.unibz.it";
    String sid = "bz10m";
    String port = "1521";
    String table = "SASA_STOPS";
    String httpFileUrl = "http://cms.sasabz.it/fileadmin/files/sasa_ge_busdata.kml";
    String localFileUrl = "sasa_ge_busdata.kml";
    DBVendor database = DBVendor.ORACLE;

    // parsing input parameters
    for (String arg : args) {
      String value = arg.substring(arg.indexOf("=") + 1);
      if (arg.startsWith("database")) {
        database = DBVendor.valueOf(value.trim().toUpperCase());
      } else if (arg.startsWith("username")) {
        username = value.trim();
      } else if (arg.startsWith("password")) {
        password = value.trim();
      } else if (arg.startsWith("url")) {
        url = value.trim();
      } else if (arg.startsWith("sid")) {
        sid = value.trim();
      } else if (arg.startsWith("port")) {
        port = value.trim();
      } else if (arg.startsWith("table")) {
        table = value.trim();
      } else if (arg.startsWith("httpFileUrl")) {
        httpFileUrl = value.trim();
      } else if (arg.startsWith("localFileUrl")) {
        localFileUrl = value.trim();
      }
    }

    System.out.println("starting import of KML");
    KMLImport app = new KMLImport(database, username, password, url, sid, port);
    // url = "http://maps.inf.unibz.it/dataExchange/upload/sasa_ge_busdata.kml";
    app.loadKMLFile(httpFileUrl, table, localFileUrl);
    System.out.println("import of KML done");

  }

}
