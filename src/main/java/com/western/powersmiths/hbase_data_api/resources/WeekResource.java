package com.western.powersmiths.hbase_data_api.resources;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.western.powersmiths.hbase_data_api.service.WeekService;
import com.western.powersmiths.hbase_data_api.model.Week;


@Path("/week")
//@Produces ({ "application/ms-excel"})
//@Consumes ({ "application/ms-excel"})

@Produces (MediaType.APPLICATION_JSON)
@Consumes (MediaType.APPLICATION_JSON)

public class WeekResource {


	WeekService weekservice = new WeekService();
	
	
	@GET
	public List<Week>  getWeeks ( @QueryParam("name") String name, @QueryParam("date") String date) {
		
		  if ((name != null) && (!name.isEmpty()) && (date != null) && (!date.isEmpty())) {
			
			return weekservice.getWeekForNameAndDate(name, date);
		}
		  
		
        if(name != null && !name.isEmpty()) {
			
			return weekservice.getWeekForName(name);
		}
		
						
		return weekservice.getAllWeeks();
		
	}
	
	
}
