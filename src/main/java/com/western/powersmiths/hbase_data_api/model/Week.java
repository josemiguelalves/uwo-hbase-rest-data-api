package com.western.powersmiths.hbase_data_api.model;


import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Week {
	
	private long id;
	private String name;
	private String datatype;
	private String date;
	private String value;
	
	public Week(){
		
		}	
	
	
	public Week(long id, String name, String datatype, String date, String value) {
		this.id = id;
		this.name = name;
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

	public void setName(String name) {
		this.name = name;
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

	public String getConsumption() {
		return value;
	}

	public void setConsumption(String value) {
		this.value = value;
	}
	
	

}
