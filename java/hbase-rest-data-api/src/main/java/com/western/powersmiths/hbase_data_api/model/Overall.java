package com.western.powersmiths.hbase_data_api.model;


public class Overall {
	
	private long id;
	private String name;
	private String datatype;
	private String date;
	private String value;
	
	
	public Overall() {
		
	}
	
	public Overall(long id, String location, String datatype, String date, String value) {
		super();
		this.id = id;
		this.name = location;
		this.datatype = datatype;
		this.date = date;
		this.value = value;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String location) {
		this.name = location;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
	
	

}
