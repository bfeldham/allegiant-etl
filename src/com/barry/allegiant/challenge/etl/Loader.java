package com.barry.allegiant.challenge.etl;

import java.sql.Connection;
import java.sql.SQLException;

public interface Loader {
	public int load(Connection dbConn) throws SQLException;
}
