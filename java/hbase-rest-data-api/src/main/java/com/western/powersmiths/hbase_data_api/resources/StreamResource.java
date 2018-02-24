package com.western.powersmiths.hbase_data_api.resources;

import com.western.powersmiths.hbase_data_api.model.Stream;
import com.western.powersmiths.hbase_data_api.service.StreamService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/stream")
@Produces({"application/json"})
@Consumes({"application/json"})
public class StreamResource
{
  StreamService streamservice = new StreamService();
  
  @GET
  public List<Stream> getStream(@QueryParam("name") String name, @QueryParam("version") String version )
  {
	  
    if ((name != null) && (!name.isEmpty()) && (version != null) && (!version.isEmpty())) {
      return this.streamservice.getStreamForNameAndVersion(name, version);
    }
        
    if ((name != null) && (!name.isEmpty())) {
      return this.streamservice.getStreamForName(name);
    }
    
    return this.streamservice.getAllStream();
  }  
}
