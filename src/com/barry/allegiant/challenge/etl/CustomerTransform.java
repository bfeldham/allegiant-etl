package com.barry.allegiant.challenge.etl;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.opencsv.CSVReader;

/**
 * This class reads the mapFile into an ordered Map (LinkedHashMap) 
 * and copies the data from the extract into a new List.
 * Any data transformation required should be coded here.
 * 
 * NOTE: The column mapping in the mapFile can be in a different 
 * order then the data in the dataFile, as long as a header row
 * exists in the CSV data file. If the CSV file does not have a 
 * header row, then the mapFile must be in the correct column 
 * order that represents the dataFile.
 * 
 * 
 * @author Barry Feldhammer
 * @version     %I%, %G%
 * @since       1.0
 * 
 *
 **/
 
public class CustomerTransform implements Transform {

	private String mapFile;
	private CSVExtract extract;
	
	private Map<String, String> columnMap = new LinkedHashMap<String, String>();
	private List<String[]> dataRecords = new ArrayList<String[]>();
	
	private static final char SEPERATOR = ',';
	private static final char QUOTE_CHAR = '"';
	
	final Log logger = LogFactory.getLog(CustomerTransform.class);
	
	@SuppressWarnings("unused")
	private CustomerTransform() {}
	
	public CustomerTransform(String mapFile, CSVExtract extract) {
		this.mapFile = mapFile;
		this.extract = extract;
	}

	@Override
	public boolean transform() throws TransformException, IOException {

		List<String[]> mapRecords;
		
		CSVReader mapReader = new CSVReader(new FileReader(mapFile), SEPERATOR, QUOTE_CHAR);	
		mapRecords = mapReader.readAll();
		logger.info(mapReader.getRecordsRead() + " records read from " + mapFile);
		mapReader.close();
		
		if (CollectionUtils.isEmpty(mapRecords)) {
			throw new TransformException("Error: Empty mapfile " + mapFile);
		} else if (mapRecords.size() < 2) {
			throw new TransformException("Error: Invalid mapfile " + mapFile);
		}
		
		List<String> dbColumns = Arrays.asList(mapRecords.get(0));
		List<String> csvHeaders = Arrays.asList(mapRecords.get(1));

		if (CollectionUtils.isEmpty(dbColumns) 
				|| CollectionUtils.isEmpty(csvHeaders)
				|| dbColumns.size() != csvHeaders.size()) {
			throw new TransformException("Error: Invalid mapfile; Column/Header count mismatch" + mapFile);
		}
		
		if (extract.hasHeaderRow()) {
			List<String> headers = extract.getDataHeaders();
			if (CollectionUtils.isEmpty(headers)
					|| headers.size() != csvHeaders.size()) {
				throw new TransformException(
						"Error: Invalid header row; Header row to mapFile mismatch"
								+ mapFile);
			}

			for (String header : headers) {
				if (csvHeaders.contains(header)) {
					columnMap.put(header, dbColumns.get(csvHeaders.indexOf(header)));
				}
			}
		} else {
			for (int i = 0; i < dbColumns.size(); i++) {
				columnMap.put(csvHeaders.get(i), dbColumns.get(i));
			}
		}
		
		// Copy data from the extract Collection to the transformation Collection
		// Data transformation can occur here
		if (CollectionUtils.isNotEmpty(extract.getDataRecords())) {
			for (String[] record : extract.getDataRecords()) {
				dataRecords.add(record);
			}
			logger.info(dataRecords.size() + " records transformed from extract");
		}
		
		return false;
	}
	
	@Override
	public Map<String, String> getColumnMap() {
		return columnMap;
	}

	@Override
	public List<String[]> getDataRecords() {
		return dataRecords;
	}
	
	public String getMapFile() {
		return mapFile;
	}

	public CSVExtract getExtract() {
		return extract;
	}
}
