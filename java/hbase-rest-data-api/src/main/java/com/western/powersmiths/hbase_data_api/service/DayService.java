package com.western.powersmiths.hbase_data_api.service;

import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.Day;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DayService
{
  private Map<Long, Day> days;
  
  public List<Day> getAllDays()
  {
 //   this.days = DatabaseClass.getAllDays();
    
    List<Day> dayAll = new ArrayList<>();
//    for (Day day : this.days.values()) {
//      dayAll.add(day);
//    }
    
    dayAll.add(new Day(1, "teste", "teste", "teste", "teste"));
    return dayAll;
  }
  
  public List<Day> getDaysForNameAndDay(String name, String date)
  {
    this.days = DatabaseClass.getDayForNameAndDate(name, date);
    
    List<Day> dayForNameAndDay = new ArrayList<>();
    for (Day day : this.days.values()) {
      dayForNameAndDay.add(day);
    }
    return dayForNameAndDay;
  }
  
  public List<Day> getDaysForName(String name)
  {
    //this.days = DatabaseClass.getDaysForName(name);
	  
	  
//    
    List<Day> dayForName = new ArrayList<>();
//    for (Day day : this.days.values()) {
//      dayForName.add(day);
//    }
    
    dayForName.add(new Day(1, "teste", "teste", "teste", "teste"));
    return dayForName;
  }
  
}
