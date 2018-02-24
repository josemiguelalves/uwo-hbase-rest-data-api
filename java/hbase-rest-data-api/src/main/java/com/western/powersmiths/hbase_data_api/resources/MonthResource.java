package com.western.powersmiths.hbase_data_api.resources;

import com.western.powersmiths.hbase_data_api.model.Month;
import com.western.powersmiths.hbase_data_api.service.MonthService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/month")
@Produces({"application/json"})
@Consumes({"application/json"})
public class MonthResource
{
  MonthService monthservice = new MonthService();
  
  @GET
  public List<Month> getMonths(@QueryParam("name") String name, @QueryParam("date") String date)
  {
    if ((name != null) && (!name.isEmpty()) && (date != null) && (!date.isEmpty())) {
      return this.monthservice.getMonthForLocantionAndMonth(name, date);
    }
    
    if ((name != null) && (!name.isEmpty())) {
      return this.monthservice.getMonthForName(name);
    }
    return this.monthservice.getAllMonths();
  }
  
  @GET
  @Path("/{monthId}")
  public Month getMonth(@PathParam("monthId") long id)
  {
    return this.monthservice.getMonth(id);
  }
}
