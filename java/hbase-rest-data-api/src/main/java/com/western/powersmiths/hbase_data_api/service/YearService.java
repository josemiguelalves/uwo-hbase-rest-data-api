package com.western.powersmiths.hbase_data_api.service;

import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class YearService
{
  private Map<Long, Year> years;
  
  public List<Year> getAllYear()
  {
    this.years = DatabaseClass.getAllYear();
    
    List<Year> yearsAll = new ArrayList<>();
    for (Year year : this.years.values()) {
      yearsAll.add(year);
    }
    return yearsAll;
  }
  
  public List<Year> getYearForLocantionAndYear(String name, String date)
  {
    this.years = DatabaseClass.getYearForNameAndMonth(name, date);
    
    List<Year> yearsForNameAndMonth = new ArrayList<>();
    for (Year year : this.years.values()) {
      yearsForNameAndMonth.add(year);
    }
    return yearsForNameAndMonth;
  }
  
  public List<Year> getYearForName(String name)
  {
    this.years = DatabaseClass.getYearForName(name);
    
    List<Year> yearsForName = new ArrayList<>();
    for (Year Year : this.years.values()) {
      yearsForName.add(Year);
    }
    return yearsForName;
  }
    
}
