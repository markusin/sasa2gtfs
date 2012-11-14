package it.unibz.inf.dis.sasa2gtfs.servlet;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.Main;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;

public class UploadServlet extends HttpServlet {
  public static final String FILE_SEP = System.getProperty("file.separator");
  static final String DATE_FORMAT = "yyyy'-'MM'-'dd'-'HH':'mm':'ss";
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

  Calendar date = Calendar.getInstance();

  @SuppressWarnings("unchecked")
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    // TODO Auto-generated method stub
    ServletFileUpload servletFileUpload = new ServletFileUpload(new DiskFileItemFactory());

    try {
      List<FileItem> fileItemLists = servletFileUpload.parseRequest(req);
      for (FileItem fileItem : fileItemLists) {
        if (fileItem.isFormField()) {
          /* The file item contains a simple name-value pair of a form field */
        } else {
          URI fileURI;
          try {
            String contextPath = getServletContext().getRealPath("/");
            URL baseDir = Thread.currentThread().getContextClassLoader().getResource("");
            String fileName = fileItem.getName().replace(" ", "");
            String suffix = fileName.substring(fileName.lastIndexOf('.'), fileName.length());
            if (suffix.equalsIgnoreCase(".zip") || suffix.equalsIgnoreCase(".tar") || suffix.equalsIgnoreCase(".gz")) {
              date.setTimeInMillis(System.currentTimeMillis());
              // here invoke ant target
              // new part is starting here ...
              DefaultLogger logger = new DefaultLogger();
              logger.setMessageOutputLevel(Project.MSG_INFO);
              logger.setErrorPrintStream(System.err);
              logger.setOutputPrintStream(System.out);
              URL buildFileURL = Thread.currentThread().getContextClassLoader().getResource("ant/build.xml");
              Project project = new Project();
              File buildFile = new File(buildFileURL.toURI());
              project.setUserProperty("session.id", req.getSession().getId());
              project.setUserProperty("ant.file", buildFile.getAbsolutePath());
              project.setUserProperty("ant.version", Main.getAntVersion());
              project.setUserProperty("properties.project.outputPath", contextPath + "/feed");

              fileName = fileName.replaceAll(suffix, "");
              fileName += "_" + dateFormat.format(date.getTime()) + suffix;
              project.setProperty("properties.project.inputFile", fileName);
              project.setProperty("properties.project.libDir", "../lib");
              project.setProperty("properties.project.classDir", "../classes");

              project.setBaseDir(new File(baseDir.toURI()));
              project.init();
              project.addBuildListener(logger);
              ProjectHelper.configureProject(project, buildFile);
              String inputPath = project.getProperty("properties.project.inputPath");
              System.out.println("Input path:" + inputPath);
              System.out.println("Base dir:" + baseDir.toExternalForm() + inputPath);
              System.out.println("Context dir:" + contextPath);
              fileURI = new URL(baseDir.toExternalForm() + inputPath + fileName).toURI();
              File uploadFile = new File(fileURI);
              uploadFile.setExecutable(false);
              fileItem.write(uploadFile);
              resp.getWriter().write("FILE PATH:" + uploadFile.getAbsolutePath() + "\n");

              try {
                project.executeTarget("sasa2gtfsWebApp");
              } catch (Exception e) {
                System.err.println(e.getMessage());
              }
              resp.sendRedirect("output.html");

              // sending mail via javax.mail
              String mailServer = project.getProperty("properties.webapp.email.server");
              String mailPort = project.getProperty("properties.webapp.email.port");
              String mailSubject = project.getProperty("properties.webapp.email.subject");
              String sender = project.getProperty("properties.webapp.email.fromAddress");
              String receiver = project.getProperty("properties.webapp.email.toAddress");
              String ccReceiver = project.getProperty("properties.webapp.email.adminAddress");

              StringBuilder body = new StringBuilder();
              body.append("A new feed is available at url: ");
              body.append("http://").append(project.getProperty("properties.webapp.hostname")).append("/");
              // body.append(project.getProperty("properties.webapp.port")).append("/");
              body.append(project.getProperty("properties.webapp.projectName")).append("/feed/");
              send(mailServer, mailPort, sender, receiver, ccReceiver, mailSubject, body.toString());
            } else {
              /*
               * resp.getWriter().write( "The file " + fileItem.getName() +
               * " is not a compressed file. Files must be of format *.zip, *.tar, *.gz ");
               */
              resp.sendRedirect("index.html");
            }
          } catch (IOException e) {
            throw new RuntimeException("Problem on storing out file");
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } catch (FileUploadException e) {
      e.printStackTrace();
    }
  }

  private void send(String mailServer, String mailPort, String sender, String receiver, String ccReceiver,
                    String subject, String body) {
    try {
      Properties props = System.getProperties();
      // -- Attaching to default Session, or we could start a new one --
      props.put("mail.smtp.host", mailServer);
      Session session = Session.getDefaultInstance(props, null);
      // -- Create a new message --
      Message msg = new MimeMessage(session);
      // -- Set the FROM and TO fields --
      msg.setFrom(new InternetAddress(sender));
      msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(receiver, false));
      msg.setRecipients(Message.RecipientType.CC, InternetAddress.parse(ccReceiver, false));
      msg.setSubject(subject);
      msg.setText(body);

      // -- Set some other header information --
      msg.setHeader("X-Mailer", "UnibzNotification");
      msg.setSentDate(new Date());
      // -- Send the message --
      Transport.send(msg);
      System.out.println("Message sent OK.");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

}