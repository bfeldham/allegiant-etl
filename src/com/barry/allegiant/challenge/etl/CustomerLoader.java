package com.barry.allegiant.challenge.etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;



import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class loads the transformed dataRecords into the database. 
 * The columMap is used to create a parameterized sql statement
 * containing only the table columns referenced in the columnMap 
 * 
 * @author Barry Feldhammer
 * @version     %I%, %G%
 * @since       1.0
 * 
 *
 **/
public class CustomerLoader implements Loader{
	private Map<String, String> columnMap = new LinkedHashMap<String, String>();
	private List<String[]> dataRecords = new ArrayList<String[]>();
	
	private static final String INSERT_STMT = "INSERT INTO customers (%columns%) VALUES (%params%)";
	private static final String COLUMNS = "%columns%";
	private static final String PARAMS = "%params%";

	final Log logger = LogFactory.getLog(CustomerLoader.class);
	
	@SuppressWarnings("unused")
	private CustomerLoader() {}
	
	public CustomerLoader(Transform transform) {
		this.columnMap = transform.getColumnMap();
		this.dataRecords = transform.getDataRecords();
	}
	
	@Override
	public int load(Connection dbConn) throws SQLException {
		int rowCount = 0;
		String sql = formatPreparedStmtSql();
		PreparedStatement insertStatement = dbConn.prepareStatement(sql);
		for (String[] record : dataRecords) {
			for (int i = 0; i < record.length; i++) {
				insertStatement.setString(i+1, record[i]);
			}	
			
			try {
				rowCount += insertStatement.executeUpdate();
			} catch (SQLException sqlEx) {
				logger.error("Error: Failed to load record " + StringUtils.join(record, ','), sqlEx);
			}
		}
		logger.info(rowCount + " rows loaded into database");
		return rowCount;
	}
		
	private String formatPreparedStmtSql() {
		String columns = StringUtils.join(columnMap.values(), ",");
		String preparedStmtSql = INSERT_STMT;
		preparedStmtSql = StringUtils.replace(preparedStmtSql, COLUMNS, columns);
		
		StringBuilder sbParams = new StringBuilder();
		for (int i = 0; i < columnMap.size(); i++) {
			if (i > 0) {
				sbParams.append(',');
			}
			sbParams.append('?');
		}
		preparedStmtSql = StringUtils.replace(preparedStmtSql, PARAMS, sbParams.toString());
		
		return (preparedStmtSql);
	}
	
}
