package xom.powersmiths.hbase.connection;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;


import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;



public class Main {
	
	
//	private static Connection conn = getConnection();
	private static HBaseAdmin admin;
	
	
//	public static Connection getConnection()
//	{
//		Configuration config = HBaseConfiguration.create();
//		config.set("hbase.zookeeper.quorum", "172.31.0.2");
//		config.set("hbase.zookeeper.property.clientPort", "2181");
//		config.set("hbase.master", "172.31.0.9:16000");
//		config.set("zookeeper.znode.parent", "/hbase-unsecure");
//		try
//		{
//			conn = ConnectionFactory.createConnection(config);
//		}
//		catch (IOException ex)
//		{
//			System.out.println("IOException : " + ex.getMessage());
//		}
//		catch (NoClassDefFoundError ex)
//		{
//			System.out.println("Error : " + ex.getMessage());
//		}
//		return conn;
//	}
//	
		
	

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws ServiceException, MasterNotRunningException, ZooKeeperConnectionException, IOException {
				
		
		Table table;
		ResultScanner rs;
		Scan scan = new Scan();
		String rowKey = null;
		Get theGet;
	    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Connection conn = null;
//		
//		Configuration config = HBaseConfiguration.create();
//		config.set("hbase.zookeeper.quorum", "172.30.0.18");
//		config.set("hbase.zookeeper.property.clientPort", "2181");
//		config.set("hbase.master", "172.30.19:16000");
//		config.set("hbase.client.retries.number", Integer.toString(3));
//        config.set("zookeeper.session.timeout", Integer.toString(60000));
//        config.set("zookeeper.recovery.retry", Integer.toString(3));
//		//config.set("zookeeper.znode.parent", "/hbase-unsecure");
        
        
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "129.100.174.152");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.master", "129.100.174.152:16000");
		config.set("hbase.client.retries.number", Integer.toString(3));
        config.set("zookeeper.session.timeout", Integer.toString(60000));
        config.set("zookeeper.recovery.retry", Integer.toString(3));
		//config.set("zookeeper.znode.parent", "/hbase-unsecure");
		
        
		
	
		try
		{
			
			conn = ConnectionFactory.createConnection(config);
		}
		catch (IOException ex)
		{
			System.out.println("IOException : " + ex.getMessage());
		}
		catch (NoClassDefFoundError ex)
		{
			System.out.println("Error : " + ex.getMessage());
		}
	
			
		
		HBaseAdmin.checkHBaseAvailable(config);
		System.out.println("Here");
//		try {
//		  table = conn.getTable(TableName.valueOf("emp"));
//			scan.setMaxVersions(5);
//		    rs = table.getScanner(scan);
//		  
//		  for (Result res : rs)
//
//			{
//			  
//				System.out.println("Here 2");
//				rowKey = Bytes.toString(res.getRow());
//				theGet = new Get(Bytes.toBytes(rowKey));
//				theGet.setMaxVersions(5);
//
//				Result result = table.get(theGet);	
//		  
//				byte [] value = result.getValue(Bytes.toBytes("personal data"),Bytes.toBytes("name"));
//
//				byte [] value1 = result.getValue(Bytes.toBytes("personal data"),Bytes.toBytes("city"));
//				
//				String name = Bytes.toString(value);
//				
//				String city = Bytes.toString(value1);
//				      
//				System.out.println("name: " + name + " city: " + city);
//	      
//			}
		  
		  
		  
//	  
//			table = conn.getTable(TableName.valueOf("powersmiths:readings_hour"));
//			scan.setMaxVersions(5);
//		    rs = table.getScanner(scan);
//		  
//		  for (Result res : rs)
//
//			{
//			  
//				System.out.println("Here 2");
//				rowKey = Bytes.toString(res.getRow());
//				theGet = new Get(Bytes.toBytes(rowKey));
//				theGet.setMaxVersions(5);
//
//				Result result = table.get(theGet);	
//		  
//				byte [] value = result.getValue(Bytes.toBytes("data_summary"),Bytes.toBytes("value"));
//
//				
//				
//				String name = Bytes.toString(value);
//				
//			
//				      
//				System.out.println("value: " + name);
//	      
//			}
//		  
		  
		  
		  
		  
//		
//		}catch(Exception e) {
//			
//			e.printStackTrace();
//		}
		
        long id = 0L;
        String date_str = null;
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);
     
        String date_day_str = null;
   
    
		
		
        try
        {
        	table = conn.getTable(TableName.valueOf("powersmiths:readings_hour"));
            rs = table.getScanner(scan);

            for (Result res : rs)
            {
                id += 1L;

                rowKey = Bytes.toString(res.getRow());
                String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

                date_str = rowKey.substring(rowKey.length() - 13);
                Date date_day = format.parse(date_str);
                date_day_str  = df.format(date_day);

                String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
                BigDecimal num = new BigDecimal(value);
                num = num.setScale(2, BigDecimal.ROUND_CEILING);
                String valueWithNoExponents = num.toPlainString();

 //               days.put(Long.valueOf(id), new Day(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

//			
//				      
//				System.out.println("value: " + name);
                
            	System.out.println("rowKey_name: " + rowKey_name + " date_day_str: " + date_day_str +  " value: " + value);
                
            }
            rs.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
		
		
		
		
	}
	

    private static  String getSubstringUntilFirstNumber(String source)
    {
        return source.split("(\\d{4}-\\d{2})")[0];
    }



}
	
	
	

