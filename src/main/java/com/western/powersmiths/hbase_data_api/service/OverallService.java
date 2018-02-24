package com.western.powersmiths.hbase_data_api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.Overall;


public class OverallService {
	
private Map<Long, Overall> overalls ;

public List<Overall> getAllOverall()
{
  this.overalls = DatabaseClass.getAllOverall();
  
  List<Overall> overallsAll = new ArrayList<>();
  for (Overall overall : this.overalls.values()) {
    overallsAll.add(overall);
  }
  return overallsAll;
}

public List<Overall> getOverallForNameAndDate(String name, String date)
{
  this.overalls = DatabaseClass.getOverallForNameAndDate(name, date);
  
  List<Overall> overallsForNameAndMonth = new ArrayList<>();
  for (Overall overall : this.overalls.values()) {
    overallsForNameAndMonth.add(overall);
  }
  return overallsForNameAndMonth;
}

public List<Overall> getOverallForName(String name)
{
  this.overalls = DatabaseClass.getOverallForName(name);
  
  List<Overall> overallsForName = new ArrayList<>();
  for (Overall Overall : this.overalls.values()) {
    overallsForName.add(Overall);
  }
  return overallsForName;
}

public List<Overall> getOverallForDataType(String datatype)
{
  List<Overall> overallsForDataType = new ArrayList<>();
  for (Overall overall : this.overalls.values()) {
    if (overall.getDatatype().equals(datatype)) {
      overallsForDataType.add(overall);
    }
  }
  return overallsForDataType;
}

public Overall getOverall(long id)
{
  return (Overall)this.overalls.get(Long.valueOf(id));
}

}
