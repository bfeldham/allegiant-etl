package com.barry.allegiant.challenge.etl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Transform {
	public Map<String, String> getColumnMap();
	public List<String[]> getDataRecords(); 
	public boolean transform() throws TransformException, IOException;
}
