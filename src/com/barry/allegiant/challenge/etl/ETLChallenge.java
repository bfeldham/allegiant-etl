package com.barry.allegiant.challenge.etl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This Java program demonstrates ETL of a CSV file into
 * as single database table. 
 * 
 * For simplicity, the target database table is dropped and 
 * created upon each execution, just prior to the data load.
 * Also, the database connection parameters and hard-coded.
 * In a production system they would be stored in an external
 * config file, or as application server variables. Also, for
 * simplicity, all columns data types of the target table are
 * VARCHAR. Converting String geo location data to Float and 
 * formatting String timestamps to TIMESTAMP data types per a
 * given locale is trivial.
 * 
 * The program contains 3 main steps as follows:
 * 1) Perform Extract
 * 2) Perform Transformation
 * 3) Perform Load
 * 
 * 
 * @author Barry Feldhammer
 * @version     %I%, %G%
 * @since       1.0
 * 
 *
 **/
public class ETLChallenge {
	
	private static final String dbConnection = "jdbc:derby://localhost/etlChallenge;create=true";
	private static final String dbUsername = null;
	private static final String dbPassword = null;
	private static final String serverName = "localhost";
	private static final int portNumber = 1527;
	
	private static final String DROP_TABLE = "DROP TABLE customers";
	private static final String CREATE_TABLE = "CREATE TABLE customers " +
			"(created_at VARCHAR(20), " +
			" first_name VARCHAR(50), " +
			" last_name VARCHAR(50), " +
			" email VARCHAR(50), " +
			" latitude VARCHAR(20), " +
			" longitude VARCHAR(20), " +
			" ip VARCHAR(15))";
	
	boolean hasColumnHeaders;
	private String mapFile;
	private String dataFile;
	
	@SuppressWarnings("unused")
	private ETLChallenge() {}
	
	public ETLChallenge(String dataFile, String mapFile, boolean hasColumnHeaders) {
		this.hasColumnHeaders = hasColumnHeaders;
		this.dataFile = dataFile;
		this.mapFile = mapFile;
	}

	public static void main(String[] args) {
		
		final Log logger = LogFactory.getLog(ETLChallenge.class);
		
		boolean hasColumnHeadersArg = true;
		String dataFileArg = null;
		String mapFileArg = null;
		
		if (ArrayUtils.isEmpty(args) || args.length < 2) {
			System.out.println("Error: Program takes at least 2 args");
			System.out.println("E.g. ETLChallenge [dataFile] [mapFile] [hasHeaderRow(default=true)]");
			System.exit(0);
		} else {
			dataFileArg = args[0];
			mapFileArg = args[1];
			if (args.length == 3) {
				hasColumnHeadersArg = Boolean.parseBoolean(args[2]);
			}
		}
		
		ETLChallenge etlChallenge = new ETLChallenge(dataFileArg, mapFileArg, hasColumnHeadersArg);
		
		// Perform Extract
		CSVExtract csvExtract = new CustomerExtract(etlChallenge.getDataFile(), etlChallenge.hasColumnHeaders);
		try {
			csvExtract.extract();
		} catch (IOException ex) {
			logger.error("Error reading dataFile: " + etlChallenge.getDataFile(), ex);
		}
		
		// Perform Transformation
		Transform transformer = new CustomerTransform(etlChallenge.getMapFile(), csvExtract);
		try {
			transformer.transform();
		} catch (Exception ex) {
			logger.error("Error transforming the extract: " + etlChallenge.getDataFile(), ex);
		}
		
		// Perform Load
		Connection dbConn = null;
		try {
			// Get DB Connection
			dbConn = ETLChallenge.getConnection();
			
			// Drop and re-create customer table
			try {
				Statement dropTableStatement = dbConn.createStatement();
				dropTableStatement.execute(ETLChallenge.DROP_TABLE);
				dropTableStatement.close();		
			} catch (SQLException sqlEx) {
				logger.error("Error dropping the customers table", sqlEx);
			}
			
			try {
				Statement createTableStatement = dbConn.createStatement();
				createTableStatement.execute(ETLChallenge.CREATE_TABLE);
				createTableStatement.close();
			} catch (SQLException sqlEx) {
				logger.error("Error creating the customers table", sqlEx);
			}

			Loader loader = new CustomerLoader(transformer);
			loader.load(dbConn);
			
		} catch (SQLException sqlEx) {
			logger.error("Error connecting to the database", sqlEx);
		} catch (Exception ex) {
			logger.error("Error loading the transformation", ex);
		} finally {
			if (dbConn != null) {
				try {
					 if(!dbConn.isClosed()) {
						 dbConn.close();
					 }
				} catch (Exception ex) {
					logger.error("Error closing the database connection", ex);
				}
			}
		}
		
		System.exit(0);
	}

	public String getDataFile() {
		return dataFile;
	}

	public String getMapFile() {
		return mapFile;
	}
		
	public static Connection getConnection() throws SQLException {
		DriverManager.registerDriver(new org.apache.derby.jdbc.ClientDriver());
		Connection dbConn = DriverManager.getConnection(ETLChallenge.dbConnection);
		return dbConn;
	}

}
