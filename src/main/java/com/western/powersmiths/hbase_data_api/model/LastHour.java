package com.western.powersmiths.hbase_data_api.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class LastHour
{
  private long id;
  private String name;
  private String datatype;
  private String date;
  private String value;
  
  public LastHour() {}
  
  public LastHour(long id, String name, String datatype, String date, String value)
  {
    this.id = id;
    this.name = name;
    this.datatype = datatype;
    this.date = date;
    this.value = value;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public long getId()
  {
    return this.id;
  }
  
  public void setId(long id)
  {
    this.id = id;
  }
  
  public String getDataType()
  {
    return this.datatype;
  }
  
  public void setDataType(String datatype)
  {
    this.datatype = datatype;
  }
  
  public String getDate()
  {
    return this.date;
  }
  
  public void setDate(String date)
  {
    this.date = date;
  }
  
  public String getValue()
  {
    return this.value;
  }
  
  public void setValue(String value)
  {
    this.value = value;
  }
}
