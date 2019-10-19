package com.barry.allegiant.challenge.etl;

public class TransformException extends Exception {
	
	/**
	 * This Exception class is intended to identify a
	 * record that caused an extract dataFile record 
	 * to fail during transformation, and identify the 
	 * offending extract row.
	 * 
	 * 
	 * @author Barry Feldhammer
	 * @version     %I%, %G%
	 * @since       1.0
	 * 
	 *
	 **/
	private static final long serialVersionUID = 6796004216699169491L;
	
	Integer recordNumber;
	String recordData;
	
	public TransformException(String msg) {
		this(msg, 0, new String());
	}

	public TransformException(String msg, int recordNumber, String recordData) {
		super(msg);
		this.recordNumber = recordNumber;
		this.recordData = recordData;
	}

}
