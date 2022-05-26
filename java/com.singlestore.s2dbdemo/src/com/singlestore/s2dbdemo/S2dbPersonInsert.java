/**
 * 
 */
package com.singlestore.s2dbdemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.sql.*;
import java.text.NumberFormat;
import java.util.*;
import com.singlestore.s2dbdemo.OutputTimeKeeper;

/**
 * @author David Ashton
 *
 */
public class S2dbPersonInsert extends Thread {

    private static final String DEFAULT_PROPS_FN = "S2dbPersonInsert.properties";
	
	private static final String DEFAULT_DB = "msdemo";
	private static final String DEFAULT_JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	private static final String DEFAULT_DB_URL = "jdbc:mariadb:sequential://172.31.57.177:3306/"+DEFAULT_DB;
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "dashtontest";	

    private static final String DEFAULT_TABLE = "person_rs";
	private static final String DEFAULT_FILE_DIR = "/home/ec2-user/projects/msdemo/files/";
	private static final int DEFAULT_BATCH_SIZE = 1000;
	
	private static  String DB = DEFAULT_DB;
	private static  String JDBC_DRIVER = DEFAULT_JDBC_DRIVER;
	private static  String DB_URL = DEFAULT_DB_URL;
    private static  String USER = DEFAULT_USER;
    private static  String PASSWORD = DEFAULT_PASSWORD;
    private static  String FILE_DIR = DEFAULT_FILE_DIR;
    
	private static String TABLE = DEFAULT_TABLE;
    private static int BATCH_SIZE = DEFAULT_BATCH_SIZE;
	
	static final String QRY_INSERT = "insert into {table} ( personid, first_name, last_name, city, state, zip, likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel ) values ".replace("{table}", TABLE);
    static final String QRY_VALUES = "(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s,%s,%s,%s,%s) ";
    static final String ENCODING = "UTF-8";

	private String jdbcDriver;
	private String jdbcUrl;
	private String jdbcUser;
	private String jdbcPassword;
	private int insertBatchSize;
	private String csvFilename;
	private StringBuilder queryString = new StringBuilder(BATCH_SIZE*300);
	private BufferedReader br;

    private static Properties prop;
	
    private static long totInserts = 0;
    
    private static synchronized long addToCounter( long totCounter ) {
    	totInserts += totCounter;
    	return totInserts;
    }

    private static String formattedNumber(long numInput) {
    	return NumberFormat.getIntegerInstance().format(numInput);
    }
	
	/**
	 * 
	 */
	public S2dbPersonInsert() {

	}
	
	public S2dbPersonInsert( String dbDriver, String dbUrl, String csvFname,
							String dbUser, String dbPassword, int batchSize) {

		this.jdbcDriver = dbDriver;
		this.jdbcUrl = dbUrl;
		this.jdbcUser = dbUser;
		this.jdbcPassword = dbPassword;
		this.insertBatchSize = batchSize;
		this.csvFilename = csvFname;
		
		try {
			
			br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilename), ENCODING));
		    br.readLine(); // skip first header line

		} catch (Exception ex) {
        	System.out.println("***ERROR*** "+ex.toString());
        	System.out.println(ex.getStackTrace().toString());
		}	
	}
	

    private static void loadProperties() {
    	
        try (InputStream input = new FileInputStream(DEFAULT_PROPS_FN)) {

            prop = new Properties();

            // load a properties file
            prop.load(input);
            System.out.println("Successfully Loaded Properties File: "+DEFAULT_PROPS_FN);
            
            DB = prop.getProperty("database", DEFAULT_DB);
            JDBC_DRIVER = prop.getProperty("jdbc_driver", DEFAULT_JDBC_DRIVER);
            DB_URL = prop.getProperty("db_url", DEFAULT_DB_URL);
            USER = prop.getProperty("user", DEFAULT_USER);
            PASSWORD = prop.getProperty("password", DEFAULT_PASSWORD);
            TABLE = prop.getProperty("table", DEFAULT_TABLE);
            FILE_DIR = prop.getProperty("file_dir", DEFAULT_FILE_DIR);
            BATCH_SIZE = Integer.parseInt(prop.getProperty("batch_size", String.valueOf(DEFAULT_BATCH_SIZE)));
            
        } catch (IOException ex) {
            System.out.println("**ERROR** Could Not Load Properties File: "+DEFAULT_PROPS_FN);
            ex.printStackTrace();
        }

        System.out.println("Database:          "+DB);
        System.out.println("JDBC Driver:       "+JDBC_DRIVER);
        System.out.println("Database URL:      "+DB_URL);
        System.out.println("Database User:     "+USER);
        System.out.println("Database Passwd:   "+"*".repeat(PASSWORD.length()));
        System.out.println("Table:             "+TABLE);
        System.out.println("Input File Dir:    "+FILE_DIR);
        System.out.println("Insert Batch Size: "+BATCH_SIZE);
        
        System.out.println("=".repeat(50));    	
    	
    }
    
	
	public void run() {
		System.out.println("Started thread for file: "+this.csvFilename);
		Connection conn = null;
		Statement  stmt = null;
	
		try {    
			// Get the connection
			conn = DriverManager.getConnection( jdbcUrl, jdbcUser, jdbcPassword ); 
			stmt = conn.createStatement (); // Create a Statement
	
			while (!getGreenLight())
				Thread.yield();
			
			//try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilename), ENCODING))) {
			try {
			    String line;
			    //br.readLine(); // skip first header line
			    
			    long insertCount = 0; 
			    long batchCount = 0;
	            
			    while ((line = br.readLine()) != null) {
			    	// process each csvfile line
			    	insertCount++;
			    	
			    	String[] c = line.split(",");
			    	String vals = String.format(QRY_VALUES, c[0],c[1],c[2],c[3],c[4],c[5],c[6],c[7],c[8],c[9],c[10],c[11]);
			    	if (queryString.length() == 0) {
			    		queryString.append(QRY_INSERT).append(vals);
			    	} else {
			    		queryString.append(',').append(vals);
			    	}
			    	
			    	if ((insertCount % this.insertBatchSize) == 0) {
			    		queryString.append(';');
			    		stmt.execute(queryString.toString());
			    		queryString.setLength(0);	// clear the queryString StringBuilder instance by resetting length
			    		batchCount++;
			    		
			    		if ((batchCount % 5) ==0) {
				    		System.out.print(".");			    			
			    		}
			    		
			    	}	
			    }
		    	if (queryString.length() > 0) {
		    		// execute insert for leftover rows
		    		queryString.append(';');
		    		stmt.execute(queryString.toString());
		    	}
		    	addToCounter(insertCount);
			} catch (Exception e) {
	        	System.out.println("***ERROR*** "+e.toString());
	        	System.out.println(e.getStackTrace().toString());				
			}
		  
		    // Close the statement
		    stmt.close();
		    stmt = null;
		  
		    // Close the local connection
		    conn.close();
		    conn = null;
		    br.close();
		    
		} catch (Exception ex) {
        	System.out.println("***ERROR*** "+ex.toString());
        	System.out.println(ex.getStackTrace().toString());
		}
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception  
	{
		loadProperties();
		Class.forName(JDBC_DRIVER);
        String dirLocation = FILE_DIR;
        
        try {
        	List<File> files = Files.list(Paths.get(dirLocation))
                                    .filter(Files::isRegularFile)
                                    .filter(path -> path.toString().endsWith(".csv"))
                                    .map(Path::toFile)
                                    .collect(Collectors.toList());
        	
            // Create the threads
            Thread[] threadList = new Thread[files.size()];
            
            for (int i = 0; i < files.size(); i++) {
                File afile = files.get(i);           	
                threadList[i] = new S2dbPersonInsert( JDBC_DRIVER, DB_URL, afile.getPath(),USER, PASSWORD, BATCH_SIZE );
                threadList[i].start();
            }
            
            Thread timerThread = new OutputTimeKeeper( 500 , "\n" );
            
            long startTime = System.currentTimeMillis();
            // Start everyone at the same time
            setGreenLight ();
            timerThread.start();

            // wait for all threads to end
            for (int i = 0; i < files.size(); i++)
            {
                threadList[i].join();
            }
            long endTime = System.currentTimeMillis();
            double durationSecs = (1.0*endTime - startTime) / 1000;
            long qps = Math.round(totInserts / durationSecs);
            timerThread.interrupt();
            
            System.out.println();
            System.out.println("=".repeat(50));
            System.out.println("Total Threads: "+ files.size());
            System.out.println("Duration Secs: "+ durationSecs);
            System.out.println("Total Inserts: "+ formattedNumber(totInserts));
            System.out.println("Insert Rows/s: "+ formattedNumber(qps));
            System.out.println("=".repeat(50));            
        } catch (Exception e)
        {
        	System.out.println("***ERROR*** "+e.toString());
        	System.out.println(e.getStackTrace().toString());
        }

	}

	static boolean greenLight = false;
	static synchronized void setGreenLight () { 
		greenLight = true;
		System.out.println("Timer Started...");
	}
	synchronized boolean getGreenLight () { return greenLight; }

}

