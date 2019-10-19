package com.barry.allegiant.challenge.etl;

import java.io.IOException;
import java.util.List;

public interface CSVExtract {
	public boolean hasHeaderRow();
	public int extract() throws IOException;
	public List<String[]> getDataRecords();
	public List<String> getDataHeaders();
}
