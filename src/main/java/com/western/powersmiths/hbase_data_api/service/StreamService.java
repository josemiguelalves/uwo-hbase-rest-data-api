package com.western.powersmiths.hbase_data_api.service;

import com.western.powersmiths.hbase_data_api.database.DatabaseClass;
import com.western.powersmiths.hbase_data_api.model.Stream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamService
{
  private Map<Long, Stream> streams;
  
  public List<Stream> getAllStream()
  {
    this.streams = DatabaseClass.getAllStream();
    
    List<Stream> allStreams = new ArrayList<>();
    for (Stream Stream : this.streams.values()) {
      allStreams.add(Stream);
    }
    return allStreams;
  }
  
  public List<Stream> getStreamForNameAndVersion(String name, String version)
  {
    this.streams = DatabaseClass.getStreamForNameAndVersion(name, version);
    
    List<Stream> streamsFornameAndVersion = new ArrayList<>();
    for (Stream Stream : this.streams.values()) {
    	streamsFornameAndVersion.add(Stream);
    }
    return streamsFornameAndVersion;
  }
  
  public List<Stream> getStreamForName(String name)
  {
    this.streams = DatabaseClass.getStreamForName(name);
    
    List<Stream> streamsForName = new ArrayList<>();
    for (Stream Stream : this.streams.values()) {
    	streamsForName.add(Stream);
    }
    return streamsForName;
  }
  
}
