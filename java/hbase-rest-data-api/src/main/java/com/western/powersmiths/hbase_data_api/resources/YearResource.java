package com.western.powersmiths.hbase_data_api.resources;

import com.western.powersmiths.hbase_data_api.model.Year;
import com.western.powersmiths.hbase_data_api.service.YearService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/year")
@Produces({"application/json"})
@Consumes({"application/json"})
public class YearResource
{
  YearService yearservice = new YearService();
  
  @GET
  public List<Year> getYears(@QueryParam("name") String name)
  {

    if ((name != null) && (!name.isEmpty())) {
      return this.yearservice.getYearForName(name);
    }
    return this.yearservice.getAllYear();
  }
  
}
