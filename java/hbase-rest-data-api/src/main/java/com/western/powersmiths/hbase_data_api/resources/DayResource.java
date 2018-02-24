package com.western.powersmiths.hbase_data_api.resources;

import com.western.powersmiths.hbase_data_api.model.Day;
import com.western.powersmiths.hbase_data_api.service.DayService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/day")
@Produces({"application/json"})
@Consumes({"application/json"})
public class DayResource
{
  DayService dayservice = new DayService();
  
  @GET
  public List<Day> getDay(@QueryParam("name") String name, @QueryParam("date") String date)
  {
    if ((name != null) && (!name.isEmpty()) && (date != null) && (!date.isEmpty())) {
      return this.dayservice.getDaysForNameAndDay(name, date);
    }
    
    if ((name != null) && (!name.isEmpty())) {
      return this.dayservice.getDaysForName(name);
    }
    return this.dayservice.getAllDays();
  }
  
}
