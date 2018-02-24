package com.western.powersmiths.hbase_data_api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.Week;


public class WeekService {
	
private Map<Long, Week> weeks;
	
public List<Week> getAllWeeks()
{
  this.weeks = DatabaseClass.getAllWeek();
  
  List<Week> weekAll = new ArrayList<>();
  for (Week week : this.weeks.values()) {
    weekAll.add(week);
  }
  return weekAll;
}

public List<Week> getWeekForNameAndDate(String name, String date)
{
  this.weeks = DatabaseClass.getWeekForNameAndDate(name, date);
  
  List<Week> weekForNameAndWeek = new ArrayList<>();
  for (Week week : this.weeks.values()) {
    weekForNameAndWeek.add(week);
  }
  return weekForNameAndWeek;
}

public List<Week> getWeekForName(String name)
{
  this.weeks = DatabaseClass.getWeekForName(name);
  
  List<Week> weekForName = new ArrayList<>();
  for (Week week : this.weeks.values()) {
    weekForName.add(week);
  }
  return weekForName;
}


}
