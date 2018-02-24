package com.western.powersmiths.hbase_data_api.resources;

import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.western.powersmiths.hbase_data_api.service.OverallService;
import com.western.powersmiths.hbase_data_api.model.Overall;


@Path("/overall")
//@Produces ({ "application/ms-excel"})
//@Consumes ({ "application/ms-excel"})

@Produces (MediaType.APPLICATION_JSON)
@Consumes (MediaType.APPLICATION_JSON)

public class OverallResource {
	
OverallService overallservice = new OverallService();
	
	
	@GET
	public List<Overall>  getOveralls (@QueryParam("name") String name, @QueryParam("date") String date ) {
		
	    if ((name != null) && (!name.isEmpty()) && (date != null) && (!date.isEmpty())) {
	        return this.overallservice.getOverallForNameAndDate(name, date);
	      }
		
        if(name != null && !name.isEmpty()) {
			
			return overallservice.getOverallForName(name);
		}
								
		return overallservice.getAllOverall();
		
	}

}
