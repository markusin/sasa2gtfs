package it.unibz.inf.dis.config;
/**
 * 
 */


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * <p>
 * The <code>PropertyUtility</code> class
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
public class Config {

  private Properties properties;
  private static Logger LOGGER = Logger.getLogger(Config.class.getPackage().getName());
  public final static String FILE_SEP = System.getProperty("file.separator");

  /**
   * <p>
   * Constructs a(n) <code>PropertyUtility</code> object.
   * </p>
   * 
   * @param configFileURI
   */
  private Config() {
    properties = new Properties();
    String baseDir = System.getProperty("user.dir") + FILE_SEP;
    String cfgFile = baseDir + "etc" + FILE_SEP + "config.xml";
    URL url;
    try {
      url = new URL("file", null, cfgFile);
      properties.loadFromXML(new FileInputStream(new File(url.toURI())));
    } catch (FileNotFoundException e) {
      LOGGER.warning(e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      LOGGER.warning(e.getMessage());
    } catch (URISyntaxException e) {
      LOGGER.severe(e.getLocalizedMessage());
    } finally {
      // add system properties
      properties.putAll(System.getProperties());
    }
  }
  
  /*
   * The SingletonHolder containing the Config object
   */
  private static class SingletonHolder {

      private final static Config INSTANCE = new Config();
  }
  
  /**
   * Returns the value of the entry specified by the passed <code>key</code>.
   * 
   * @param key    the key that identifies the entry
   * @return       the value of the entry
   */
  public static String getProperty(String key, String defaultValue) {
      String value = SingletonHolder.INSTANCE.properties.getProperty(key);
      return value==null ? defaultValue : value;
  }
  
  public static String getProperty(String key) {
    return SingletonHolder.INSTANCE.properties.getProperty(key);
  }
  
  /**
   * 
   * <p>Method replacePropertyFile</p>
   * @param uri
   */
  public static void replacePropertyFile(URI uri) {
    FileInputStream inputStream;
    try {
      inputStream = new FileInputStream(new File(uri));
      SingletonHolder.INSTANCE.properties.loadFromXML(inputStream);
    } catch (FileNotFoundException e) {
      LOGGER.warning(e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      LOGGER.warning(e.getMessage());
    } finally {
      // add system properties
      SingletonHolder.INSTANCE.properties.putAll(System.getProperties());
    }
  }

}
