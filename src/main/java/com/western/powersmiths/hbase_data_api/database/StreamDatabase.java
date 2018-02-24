
package com.western.powersmiths.hbase_data_api.database;

import com.western.powersmiths.hbase_data_api.model.Stream;
import com.western.powersmiths.hbase_data_api.model.Day;
import com.western.powersmiths.hbase_data_api.model.Week;
import com.western.powersmiths.hbase_data_api.model.LastHour;
import com.western.powersmiths.hbase_data_api.model.Month;
import com.western.powersmiths.hbase_data_api.model.Year;
import com.western.powersmiths.hbase_data_api.model.Overall;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.DateTimeConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class StreamDatabase
{

	private static Map<Long, Stream> streams = new HashMap<>();
	private static Connection conn = getConnection();
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static Connection getConnection()
	{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "192.168.56.100");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.master", "192.168.56.100:16000");
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
		return conn;
	}

	public static Map<Long, Stream> getAllStream()
	{

		long id = 0L;
		String date_str = null;
		Scan scan = new Scan();
		String rowKey = null;
		Get theGet;
		List<Cell> date_time_res;
		List<Cell> values_res;
		ArrayList<String> dates = new ArrayList<>();
		ArrayList<String> results = new ArrayList<>();

		streams.clear();
		try
		{
			Table table = conn.getTable(TableName.valueOf("powersmiths:stream"));
			scan.setMaxVersions(5);
			ResultScanner rs = table.getScanner(scan);

			for (Result res : rs)

			{

				rowKey = Bytes.toString(res.getRow());
				theGet = new Get(Bytes.toBytes(rowKey));
				theGet.setMaxVersions(5);

				Result result = table.get(theGet);				

				//loop for result
				date_time_res = result.getColumnCells(Bytes.toBytes("raw_data"), Bytes.toBytes("datetime"));		    
				values_res= result.getColumnCells(Bytes.toBytes("raw_data"), Bytes.toBytes("value"));	


				for ( Cell cell : date_time_res )
				{

					byte[] datetime = CellUtil.cloneValue(cell);
					date_str = Bytes.toString(datetime);						
					dates.add(date_str);
				}


				for ( Cell cell : values_res )
				{

					byte[] value_byte = CellUtil.cloneValue(cell);
					String value =  Bytes.toString(value_byte);
					BigDecimal num = new BigDecimal(value);
					num = num.setScale(2, BigDecimal.ROUND_CEILING);
					String valueWithNoExponents = num.toPlainString();
					results.add(valueWithNoExponents);

				}

				for (int j=0; j< dates.size(); j++)
				{
					id += 1L;
					streams.put(Long.valueOf(id), new Stream(id, rowKey , "not avaiable", dates.get(j) , results.get(j)));

				}

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return streams;					
	}


	public static Map<Long, Stream> getStreamForName(String name)
	{
		long id = 0L;
		String date_str = null;
		Scan scan = new Scan();
		String rowKey = null;

		streams.clear();

		try
		{
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name));
			scan.setFilter(filter);

			Table table = conn.getTable(TableName.valueOf("powersmiths:stream"));
			ResultScanner rs = table.getScanner(scan);					

			for (Result res : rs)
			{
				id += 1L;
				rowKey = Bytes.toString(res.getRow());
				date_str = Bytes.toString(res.getValue(Bytes.toBytes("raw_data"), Bytes.toBytes("datetime")));

				String value = Bytes.toString(res.getValue(Bytes.toBytes("raw_data"), Bytes.toBytes("value")));
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				streams.put(Long.valueOf(id), new Stream(id, rowKey_name, "not avaiable", date_str , valueWithNoExponents));
			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return streams;
	}


	public static Map<Long, Stream> getStreamForNameAndVersion(String name, String version)
	{
		long id = 0L;
		String date_str = null;
		Scan scan = new Scan();
		String rowKey = null;
		Get theGet;
		List<Cell> date_time_res;
		List<Cell> values_res;
		ArrayList<String> dates = new ArrayList<>();
		ArrayList<String> results = new ArrayList<>(); 
		int version_int;

		streams.clear();
		try
		{

			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name));
			scan.setFilter(filter);
			Table table = conn.getTable(TableName.valueOf("powersmiths:stream"));
			scan.setMaxVersions(5);
			ResultScanner rs = table.getScanner(scan);

			for (Result res : rs)

			{

				rowKey = Bytes.toString(res.getRow());
				theGet = new Get(Bytes.toBytes(rowKey));
				theGet.setMaxVersions(5);
				Result result = table.get(theGet);				

				//loop for result
				date_time_res = result.getColumnCells(Bytes.toBytes("raw_data"), Bytes.toBytes("datetime"));		    
				values_res = result.getColumnCells(Bytes.toBytes("raw_data"), Bytes.toBytes("value"));	


				for ( Cell cell : date_time_res )
				{

					byte[] datetime = CellUtil.cloneValue(cell);
					date_str = Bytes.toString(datetime);						
					dates.add(date_str);
				}


				for ( Cell cell : values_res )
				{

					byte[] value_byte = CellUtil.cloneValue(cell);

					String value =  Bytes.toString(value_byte);
					BigDecimal num = new BigDecimal(value);
					num = num.setScale(2, BigDecimal.ROUND_CEILING);
					String valueWithNoExponents = num.toPlainString();
					results.add(valueWithNoExponents);

				}

				version_int = Integer.parseInt(version);				



				if 	(version_int > 0) {	
					version_int = version_int -1;
					id += 1L;
					streams.put(Long.valueOf(id), new Stream(id, rowKey , "not avaiable", dates.get(version_int) , results.get(version_int)));
				} 

				else 

					if 	(version_int == 0) {	

						for (int j=0; j< dates.size(); j++)
						{
							id += 1L;
							streams.put(Long.valueOf(id), new Stream(id, rowKey , "not avaiable", dates.get(j) , results.get(j)));

						}
					}

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return streams;					
	}


	public static String getSubstringUntilFirstNumber(String source)
	{
		return source.split("(\\d{4}-\\d{2})")[0];
	}
}

