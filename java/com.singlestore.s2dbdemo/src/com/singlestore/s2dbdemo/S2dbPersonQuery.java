package com.singlestore.s2dbdemo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;

public class S2dbPersonQuery {

	private static final String DEFAULT_DB = "msdemo";
	private static final String DEFAULT_JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	private static final String DEFAULT_DB_URL = "jdbc:mariadb:sequential://172.31.57.177:3306/"+DEFAULT_DB;
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "dashtontest";

    private static final String DEFAULT_THREADS = "1,5,10,20,50";
    private static final int DEFAULT_DURATION = 10;
    private static final String DEFAULT_TABLE = "person_rs";
    
	private static  String DB = DEFAULT_DB;
	private static  String JDBC_DRIVER = DEFAULT_JDBC_DRIVER;
	private static  String DB_URL = DEFAULT_DB_URL;
    private static  String USER = DEFAULT_USER;
    private static  String PASSWORD = DEFAULT_PASSWORD;
    
    private static int[] THREAD_COUNT = {1,5,10,20,50};
    private static int DURATION_SECS = DEFAULT_DURATION;
    
    private static String TABLE = DEFAULT_TABLE;
    
    private static final int ID_COUNT = 1000;
    private static final String ID_COUNT_STR = Integer.toString(ID_COUNT);
    private static final String QRY = "select * from {table} where personid={id}".replace("{table}", TABLE);
    private static final String QRY_ID = "select personid from {table} order by personid limit {count}".replace("{table}", TABLE).replace("{count}", ID_COUNT_STR);
    private static final String DEFAULT_PROPS_FN = "S2dbPersonQuery.properties";

    private static long totQueries = 0;

    private static ArrayList<String> ids = new ArrayList<String>();
    
    private static Properties prop;
    
    private static synchronized long addToCounter( long totCounter ) {
    	totQueries += totCounter;
    	return totQueries;
    }
    
    private static String formattedNumber(long numInput) {
    	return NumberFormat.getIntegerInstance().format(numInput);
    }
    
    private static void executeSQL(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        	//ResultSet rs = stmt.executeQuery(sql);
        	//rs.next();
        	//System.out.println(rs.getLong("personid")+","+rs.getString("first_name"));
        } catch (SQLException e) {
            System.out.println("**ERROR** \n"+e.toString());
        }
    }
    
    private static void loadProperties() {
    	
        try (InputStream input = new FileInputStream(DEFAULT_PROPS_FN)) {

            prop = new Properties();

            // load a properties file
            prop.load(input);
            System.out.println("Successfully Loaded Properties File: "+DEFAULT_PROPS_FN);
            
            THREAD_COUNT = Arrays.stream(prop.getProperty("threads", DEFAULT_THREADS).split(",")).mapToInt(Integer::parseInt).toArray();
            DURATION_SECS = Integer.parseInt(prop.getProperty("duration", String.valueOf(DEFAULT_DURATION)));
            DB = prop.getProperty("database", DEFAULT_DB);
            JDBC_DRIVER = prop.getProperty("jdbc_driver", DEFAULT_JDBC_DRIVER);
            DB_URL = prop.getProperty("db_url", DEFAULT_DB_URL);
            USER = prop.getProperty("user", DEFAULT_USER);
            PASSWORD = prop.getProperty("password", DEFAULT_PASSWORD);
            TABLE = prop.getProperty("table", DEFAULT_TABLE);            
            
        } catch (IOException ex) {
            System.out.println("**ERROR** Could Not Load Properties File: "+DEFAULT_PROPS_FN);
            ex.printStackTrace();
        }
        System.out.println("Test Duration (s): "+DURATION_SECS);            
        System.out.println("Test with THREADS: "+Arrays.toString(THREAD_COUNT));
        System.out.println("Database:          "+DB);
        System.out.println("JDBC Driver:       "+JDBC_DRIVER);
        System.out.println("Database URL:      "+DB_URL);
        System.out.println("Database User:     "+USER);
        System.out.println("Database Passwd:   "+"*".repeat(PASSWORD.length()));
        System.out.println("Table:             "+TABLE);
        System.out.println("=".repeat(50));    	
    	
    }
    
    
    private static void ResetEnvironment() throws SQLException {
        Properties p = new Properties();
        p.put("user", USER);
        p.put("password", PASSWORD);
        try (Connection conn = DriverManager.getConnection(DB_URL, p)) {
        	// Do any initialization required
        	Statement stmt = conn.createStatement();
        	ResultSet rs = stmt.executeQuery(QRY_ID);
        	while (rs.next()) {
        		ids.add(rs.getString("personid"));
        	}
        	conn.close();
        }
    }
    
    
    private static void worker() {
    	
    	//System.out.println("Starting Worker ...");
        Properties properties = new Properties();
        properties.put("user", USER);
        properties.put("password", PASSWORD);
        try (Connection conn = DriverManager.getConnection(DB_URL, properties)) {
            executeSQL(conn, "USE "+DB);
            int counter = 0;
            int randomIndex = 0;
            int maxIndex = ids.size()-1;
            
            while (!Thread.interrupted()) {
            	randomIndex = ThreadLocalRandom.current().nextInt(0,maxIndex);
                executeSQL(conn, QRY.replace("{id}", ids.get(randomIndex)));
                counter++;
            }
            //System.out.println("queried "+counter);
            addToCounter(counter);
            conn.close();
        } catch (SQLException e) {
            System.out.println("**ERROR** \n"+e.toString());
        }
    }
    
    
    public static void runBenchmark(int threadCount) throws ClassNotFoundException, SQLException, InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    worker();
                }
            });
        }
        System.out.println("Started "+threadCount+" worker threads ...");
        Thread.sleep(DURATION_SECS*1000);
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            System.err.println("Pool did not terminate");
        }
        long qps = totQueries / DURATION_SECS;
        System.out.println("=".repeat(50));
        System.out.println("Total Threads: "+ threadCount);
        System.out.println("Duration Secs: "+ DURATION_SECS);
        System.out.println("Total Queries: "+ formattedNumber(totQueries));
        System.out.println("Total QPS:     "+ formattedNumber(qps));
        System.out.println("=".repeat(50));
    }
    
    
    
	/**
	 * @param args
	 * 
	 */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {

    	
        Class.forName(JDBC_DRIVER);
        loadProperties();
        ResetEnvironment();
        for (int i =0;i < THREAD_COUNT.length;i++)
        {
        	runBenchmark(THREAD_COUNT[i]);
        }	    
    }    
}