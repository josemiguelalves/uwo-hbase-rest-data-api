package com.western.powersmiths.hbase_data_api.service;

import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.LastHour;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LastHourService
{
  private Map<Long, LastHour> lastHours;
  
  public List<LastHour> getAllLastHour()
  {
    this.lastHours = DatabaseClass.getAllLastHour();
    
    List<LastHour> allLastHours = new ArrayList<>();
    for (LastHour LastHour : this.lastHours.values()) {
      allLastHours.add(LastHour);
    }
    return allLastHours;
  }
  
  public List<LastHour> getLastHourForNameAndHour(String name, String date)
  {
    this.lastHours = DatabaseClass.getLastHourForNameAndHour(name, date);
    
    List<LastHour> lastHoursForNameAndDate = new ArrayList<>();
    for (LastHour LastHour : this.lastHours.values()) {
      lastHoursForNameAndDate.add(LastHour);
    }
    return lastHoursForNameAndDate;
  }
  
  public List<LastHour> getLastHourForName(String name)
  {
    this.lastHours = DatabaseClass.getLastHourForName(name);
    
    List<LastHour> lastHoursForName = new ArrayList<>();
    for (LastHour LastHour : this.lastHours.values()) {
      lastHoursForName.add(LastHour);
    }
    return lastHoursForName;
  }
 
  
}
