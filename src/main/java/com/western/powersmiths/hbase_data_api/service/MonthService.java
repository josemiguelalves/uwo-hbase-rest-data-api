package com.western.powersmiths.hbase_data_api.service;

import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MonthService
{
  private Map<Long, Month> months;
  
  public List<Month> getAllMonths()
  {
    this.months = DatabaseClass.getAllMonths();
    
    List<Month> monthsAll = new ArrayList<>();
    for (Month month : this.months.values()) {
      monthsAll.add(month);
    }
    return monthsAll;
  }
  
  public List<Month> getMonthForLocantionAndMonth(String name, String date)
  {
    this.months = DatabaseClass.getMonthForNameAndMonth(name, date);
    
    List<Month> monthsForNameAndMonth = new ArrayList<>();
    for (Month month : this.months.values()) {
      monthsForNameAndMonth.add(month);
    }
    return monthsForNameAndMonth;
  }
  
  public List<Month> getMonthForName(String name)
  {
    this.months = DatabaseClass.getMonthsForName(name);
    
    List<Month> monthsForName = new ArrayList<>();
    for (Month month : this.months.values()) {
      monthsForName.add(month);
    }
    return monthsForName;
  }
  
  public List<Month> getMonthForDataType(String datatype)
  {
    List<Month> monthsForDataType = new ArrayList<>();
    for (Month month : this.months.values()) {
      if (month.getDatatype().equals(datatype)) {
        monthsForDataType.add(month);
      }
    }
    return monthsForDataType;
  }
  
  public Month getMonth(long id)
  {
    return (Month)this.months.get(Long.valueOf(id));
  }
}
