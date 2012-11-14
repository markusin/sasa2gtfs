This is a tool to convert the SASA data into the Google Transit Feed Format.

Requirements:
- Java (JRE 6.0) 
- Apache Ant (make tool)
- Oracle Database with Spatial extension: 
	- sqlplus (SQL command shell) 
	- sqlldr (SQL loader)

Input:
- a ZIP file packed with the following files:
	- lineeCorse.csv (representing trips)
	- linee.csv (representing routes)
	- orariPassaggio.csv (representing schedule)	
	- sasa_ge_busdata.kml (representing stops) 
by convention the first line of the CSV files has to have a header(description of the columns)

Output: 
- a compressed zip file (sasaGTFS.zip) containing the following files:
  - stops.txt (bus stops)
  - routes.txt (bus routes)
  - trips.txt (bus trips)
  - stops_times.txt (schedule)
  - calendar.txt (calendar)
  - calendar_dates.txt (calendar_exception)

The configuration file is located in the folder ant/properties.xml.

In order to use the tool go into the directory ant and by typing the command
"ant" is shows you the possible options to run the tool: e.g. by launching the complete generation
type the command "ant run" (if you want to run it in quiet mode use "ant run -q").