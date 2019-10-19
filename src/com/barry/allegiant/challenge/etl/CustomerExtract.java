package com.barry.allegiant.challenge.etl;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.opencsv.CSVReader;

/**
 * This class reads the dataFile into a List (List<String[]>) 
 * and copies the header data, if it exists, into a seperate List.
 * This class demonstrates the use of the com.opencsv.CSVReader
 * utility, which is popular with Sailpoint developers 
 * 
 * @author Barry Feldhammer
 * @version     %I%, %G%
 * @since       1.0
 * 
 *
 **/
public class CustomerExtract implements CSVExtract {
	
	boolean hasHeaderRow;
	private String dataFile;
	
	List<String[]> dataRecords;
	List<String> dataHeaders;

	private static final char SEPERATOR = ',';
	private static final char QUOTE_CHAR = '"';
	
	final Log logger = LogFactory.getLog(CustomerExtract.class);

	@SuppressWarnings("unused")
	private CustomerExtract() {}
	
	public CustomerExtract(String dataFile, boolean hasHeaderRow) {
		this.dataFile = dataFile;
		this.hasHeaderRow = hasHeaderRow;
	}
	
	
	@Override
	public int extract() throws IOException {
		
		CSVReader dataReader = new CSVReader(new FileReader(dataFile), SEPERATOR, QUOTE_CHAR);
		dataRecords = dataReader.readAll();
		logger.info(dataReader.getRecordsRead() + " records read from " + dataFile);
		dataReader.close();

		if (CollectionUtils.isEmpty(dataRecords)) {
			return 0;
		} else if (hasHeaderRow) {
			dataHeaders = Arrays.asList(dataRecords.remove(0));
			logger.info("Header record removed from extract data file");
		}
		
		return dataRecords.size();
	}

	public String getDataFile() {
		return dataFile;
	}

	@Override
	public List<String[]> getDataRecords() {
		return dataRecords;
	}

	@Override
	public List<String> getDataHeaders() {
		return dataHeaders;
	}
	
	@Override
	public boolean hasHeaderRow() {
		return hasHeaderRow;
	}

}
