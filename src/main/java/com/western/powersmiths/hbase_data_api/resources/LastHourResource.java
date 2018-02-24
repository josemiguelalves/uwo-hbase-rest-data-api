package com.western.powersmiths.hbase_data_api.resources;

import com.western.powersmiths.hbase_data_api.model.LastHour;
import com.western.powersmiths.hbase_data_api.service.LastHourService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/lasthour/h")
@Produces({"application/json"})
@Consumes({"application/json"})
public class LastHourResource
{
  LastHourService lastHourservice = new LastHourService();
  
  @GET
  public List<LastHour> getLastHours( @QueryParam("name") String name, @QueryParam("date") String date)
  {
    if ((name != null) && (!name.isEmpty()) && (date != null) && (!date.isEmpty())) {
      return this.lastHourservice.getLastHourForNameAndHour(name, date);
    }

    if ((name != null) && (!name.isEmpty())) {
      return this.lastHourservice.getLastHourForName(name);
    }
    return this.lastHourservice.getAllLastHour();
  }
  
}
