package it.unibz.inf.dis.db;

import it.unibz.inf.dis.config.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleResultSet;

/**
 * <p>
 * The <code>JDBCConnector</code> class manages the connection to the database.
 * </p>
 * <p>
 * <a href="http://www.http://www.inf.unibz.it/dis">Database Information Systems - Research Group</a>
 * </p>
 * <p>
 * Dominikanerplatz 39100 Bolzano, Italy.
 * </p>
 * <p>
 * </p>
 * 
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 1.0
 */
public class JDBCConnector {

  private static Logger LOGGER = Logger.getLogger(JDBCConnector.class.getName());

  private String username = "ISO_DEV";
  private String password = "ISO_DEV";
  private String host = "bz10m.inf.unibz.it";
  private String sid = "orcl11";
  private String port = "1521";

  private Connection connection = null;

  public String getName() {
    return username;
  }

  public void setName(String name) {
    this.username = name;
  }

  public String getPw() {
    return password;
  }

  public void setPw(String pw) {
    this.password = pw;
  }

  private Statement stmt = null;

  /**
   * creates a connection to a specified database, creates an updatable oracle result set if no parameters are passed,
   * looks for configuration file and takes predefined connection parameters
   * 
   * @param uname
   * @param passw
   * @param dburl
   * @throws SQLException
   */
  public JDBCConnector(DBVendor database, String userName, String password, String host, String db, String port) {
    String url;
    try {
      if (database.equals(DBVendor.POSTGRESQL)) {
        Class.forName("org.postgresql.Driver"); // load the driver
        userName = (userName == null) ? Config.getProperty("org.postgresql.username") : userName;
        password = (password == null) ? Config.getProperty("org.postgresql.password") : password;
        url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
      } else {
        Class.forName("oracle.jdbc.driver.OracleDriver"); // load the driver
        userName = (userName == null) ? Config.getProperty("oracle.jdbc.username") : userName;
        password = (password == null) ? Config.getProperty("oracle.jdbc.password") : password;
        url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + db;
      }
      System.out.println("Connecting to dabase: " + url);
      System.out.println("DB username: " + userName);
      System.out.println("DB password: " + password);
      connection = DriverManager.getConnection(url, userName, password);
      connection.setAutoCommit(false);
      stmt = connection.createStatement(OracleResultSet.TYPE_SCROLL_INSENSITIVE, OracleResultSet.CONCUR_UPDATABLE);
    } catch (SQLException e) {
      System.err.println("Unable to connect to the database: " + e);
      System.exit(1);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    /*
     * try { this.username = uname != null ? uname : Config.getProperty("db.username", "scott"); this.password = passw
     * != null ? passw : Config.getProperty("db.password", "tiger"); this.host = host != null ? host :
     * Config.getProperty("db.host", "localhost"); this.sid = sid != null ? sid : Config.getProperty("db.sid", "orcl");
     * this.port = port != null ? port : Config.getProperty("db.port", "1521"); DriverManager.registerDriver(new
     * OracleDriver()); conn = DriverManager.getConnection(getConnectionURL(), username, password); stmt =
     * conn.createStatement(OracleResultSet.TYPE_SCROLL_INSENSITIVE, OracleResultSet.CONCUR_UPDATABLE); }
     */
  }

  /**
   * default constructor, reads connection string and credentials from configuration file
   * 
   * @throws SQLException
   */
  public JDBCConnector() throws SQLException {
    DriverManager.registerDriver(new OracleDriver());
    connection = DriverManager.getConnection(getConnectionURL(), username, password);
    stmt = connection.createStatement(OracleResultSet.TYPE_SCROLL_INSENSITIVE, OracleResultSet.CONCUR_UPDATABLE);
  }

  /**
   * @return connection to database
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * @return statement with updatable, TYPE_SCROLL_INSENSITIVE result set
   */
  public Statement getStatement() {
    return stmt;
  }

  /**
   * closes statement and connection
   */
  public void close() {
    try {
      if (!stmt.isClosed())
        stmt.close();
    } catch (SQLException e1) {
      e1.printStackTrace();
      LOGGER.severe("Problems during closing SQL statement: " + e1.getMessage());
    }
    try {
      if (!connection.isClosed())
        connection.close();
    } catch (SQLException e2) {
      e2.printStackTrace();
      LOGGER.severe("Problems during closing SQL conncetion: " + e2.getMessage());
    }

  }

  /**
   * executes a non-query & commits on successful completion
   * 
   * @param query
   * @return
   */
  public boolean executeNonQuery(String query) {
    boolean success = false;
    try {
      stmt.execute(query);
      connection.commit();
      success = true;
    } catch (SQLException e) {
      LOGGER.severe("Problems when invoking query: " + query + ". Reason:" + e.getLocalizedMessage());
      e.printStackTrace();
    }
    return success;
  }

  /**
   * <p>
   * Method getConnectionURL
   * </p>
   * returns the connection string
   * 
   * @return the concatenated connection string consisting of hostname, portnumber and sid
   */
  private String getConnectionURL() {
    String url = "jdbc:oracle:thin:@" + this.host + ":" + this.port + ":" + this.sid;
    System.out.println("Connecting to url: " + url);
    return url;
  }

}
