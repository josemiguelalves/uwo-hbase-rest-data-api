package com.western.powersmiths.hbase_data_api.database;

import com.western.powersmiths.hbase_data_api.model.Day;
import com.western.powersmiths.hbase_data_api.model.LastHour;
import com.western.powersmiths.hbase_data_api.model.Month;
import com.western.powersmiths.hbase_data_api.model.Year;
import com.western.powersmiths.hbase_data_api.model.Overall;
import com.western.powersmiths.hbase_data_api.model.Stream;
import com.western.powersmiths.hbase_data_api.model.Week;

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

public class DatabaseClass
{
	private static Map<Long, LastHour> lastHours = new HashMap<>();
	private static Map<Long, Day> days = new HashMap<>();
	private static Map<Long, Week> weeks = new HashMap<>();
	private static Map<Long, Month> months = new HashMap<>();
	private static Map<Long, Year> years = new HashMap<>();
	private static Map<Long, Overall> overalls = new HashMap<>();
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
		Table table;
		ResultScanner rs;

		streams.clear();
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:stream"));
			scan.setMaxVersions(5);
		    rs = table.getScanner(scan);

			for (Result res : rs)

			{

				rowKey = Bytes.toString(res.getRow());
				theGet = new Get(Bytes.toBytes(rowKey));
				theGet.setMaxVersions(5);

				Result result = table.get(theGet);				

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
		PrefixFilter filter;
		Table table;
		ResultScanner rs;

		streams.clear();

		try
		{
			filter = new PrefixFilter(Bytes.toBytes(name));
			scan.setFilter(filter);
			table = conn.getTable(TableName.valueOf("powersmiths:stream"));
			rs = table.getScanner(scan);					

			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());					
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
				
				date_str = Bytes.toString(res.getValue(Bytes.toBytes("raw_data"), Bytes.toBytes("datetime")));
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("raw_data"), Bytes.toBytes("value")));
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
		PrefixFilter filter;
		Table table;
		ResultScanner rs;

		streams.clear();
		try
		{

		    filter = new PrefixFilter(Bytes.toBytes(name));
			scan.setFilter(filter);
			table = conn.getTable(TableName.valueOf("powersmiths:stream"));
			scan.setMaxVersions(5);
			rs= table.getScanner(scan);

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


	public static Map<Long, LastHour> getAllLastHour()
	{
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_last_hour_str = null;
		String rowKey = null;
		Table table;
		ResultScanner rs;

		lastHours.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:realtime"));
			rs = table.getScanner(scan);

			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
				
				date_str = rowKey.substring(rowKey.length() - 16);
				Date date_last_hour = format.parse(date_str);
				date_last_hour_str   = df.format(date_last_hour);

				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				lastHours.put(Long.valueOf(id), new LastHour(id, rowKey_name, "not avaiable", date_last_hour_str , valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return lastHours;
	}
	

	public static Map<Long, LastHour> getLastHourForNameAndHour(String name, String date)
	{
		String name_query = null;
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_last_hour_str = null;
		String rowKey = null;
		PrefixFilter filter;
		Table table;
		ResultScanner rs;
		
		name_query = name + " " + date;		
	    filter = new PrefixFilter(Bytes.toBytes(name_query));				
		scan.setFilter(filter);
		
		lastHours.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:realtime"));
			rs = table.getScanner(scan);
			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());				
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				date_str = rowKey.substring(rowKey.length() - 16);
				Date date_last_hour = format.parse(date_str);
				date_last_hour_str   = df.format(date_last_hour);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				lastHours.put(Long.valueOf(id), new LastHour(id, rowKey_name, "not avaiable", date_last_hour_str , valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return lastHours;
	}

	
	public static Map<Long, LastHour> getLastHourForName(String name)
	{
		String name_query = null;
		long id = 0L;
		String date_str = null;
		String date_last_hour_str = null;
		String month_str = null;
		String rowKey = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
		Scan scan = new Scan();	 
		LocalDateTime localdate = new LocalDateTime();		
		int day = localdate .getDayOfMonth();
		int month = localdate .getMonthOfYear();
		int year = localdate .getYear();
		int hour = localdate.getHourOfDay();
		String year_str = String.valueOf(year);
		String day_str = String.valueOf(day);
		String hour_str = String.valueOf(hour);
		PrefixFilter filter;
		Table table;
		ResultScanner rs;
		
		month_str = String.valueOf(month);

		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		}
		if (day < 10) {
			day_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
		}
		if (hour < 10) {
			hour_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
		}
		
		month_str = "07";
		year_str = "2017";
		day_str = "12";
		hour_str = "14";

		name_query = name + " " + year_str + "-" + month_str + "-" + day_str + " " + hour_str;
	    filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);

		lastHours.clear();

		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:realtime"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;

				rowKey = Bytes.toString(res.getRow());								
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				date_str = rowKey.substring(rowKey.length() - 16);
				Date date_last_hour = format.parse(date_str);
				date_last_hour_str   = df.format(date_last_hour);
				
				float value = Bytes.toFloat(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));				
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				lastHours.put(Long.valueOf(id), new LastHour(id, rowKey_name, "not avaiable", date_last_hour_str , valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return lastHours;
	}

	public static Map<Long, Day> getAllDays()
	{
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_day_str = null;
		String rowKey = null;
		Table table;
		ResultScanner rs;

		days.clear();

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

				days.put(Long.valueOf(id), new Day(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return days;
	}
	

	public static Map<Long, Day> getDayForNameAndDate(String name, String date)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_day_str = null;
		String rowKey = null;
		PrefixFilter filter;
		Table table;
		ResultScanner rs;

		name_query = name + " " + date;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);

		days.clear();
		
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
				date_day_str    = df.format(date_day);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));	
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				days.put(Long.valueOf(id), new Day(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return days;
	}

	
	public static Map<Long, Day> getDaysForName(String name)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_day_str = null;		
		String month_str = null;
		String rowKey = null;
		String day_str = null;
		LocalDate localdate = new LocalDate();
		int day = localdate.getDayOfMonth();
		int month = localdate.getMonthOfYear();
		int year = localdate.getYear();
		String year_str = String.valueOf(year);
		PrefixFilter filter;
		Table table;
		ResultScanner rs;
		
		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		}
		if (day < 10) {
			day_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
		}
		month_str = "08";
		year_str = "2015";
		day_str = "05";

		name_query = name + " " + year_str + "-" + month_str + "-" + day_str;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		days.clear();

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
				date_day_str    = df.format(date_day);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				days.put(Long.valueOf(id), new Day(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return days;
	}


	public static Map<Long, Week> getAllWeek()
	{
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_day_str = null;
		String rowKey = null;
		String string = "2013-01-03";
		LocalDate now = new LocalDate(string);
		LocalDate monday = now.withDayOfWeek(DateTimeConstants.MONDAY);
		Table table;
		ResultScanner rs;

		weeks.clear();

		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
			rs = table.getScanner(scan);

			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();						
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_day = format.parse(date_str);
				date_day_str  = df.format(date_day);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				if(date_str.equals(monday.toString()))

					weeks.put(Long.valueOf(id), new Week(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return weeks;
	}

	
	public static Map<Long, Week> getWeekForNameAndDate(String name, String date)
	{
		String rowKey = null;
		String name_query = null;
		String date_day_str = null;		
		String month_str = null;
		String day_str = null;
		Scan scan = new Scan();
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		LocalDate localdate = new LocalDate(date);
		LocalDate monday = localdate.withDayOfWeek(DateTimeConstants.MONDAY);
		int month = monday .getMonthOfYear();
		int year = monday .getYear();	
		int day = monday.getDayOfMonth();
		String year_str = String.valueOf(year);	
		PrefixFilter filter;
		Table table;
		ResultScanner rs;
		

		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		} else
			month_str = String.valueOf(month);
		
		weeks.clear();

		for (int d =0; d<7; d++) {

			if (day < 10) {
				day_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
			} else
				day_str =  String.valueOf(day);		

			name_query = name + " " + year_str + "-" + month_str + "-" + day_str;
			
		    filter = new PrefixFilter(Bytes.toBytes(name_query));
			scan.setFilter(filter);

			try
			{
				table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
				rs = table.getScanner(scan);
				
				for (Result res : rs)
				{
					id += 1L;
					
					rowKey = Bytes.toString(res.getRow());
					String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
										
					date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
					Date date_day = format.parse(date_str);
					date_day_str    = df.format(date_day);
					
					String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
					BigDecimal num = new BigDecimal(value);
					num = num.setScale(2, BigDecimal.ROUND_CEILING);
					String valueWithNoExponents = num.toPlainString();

					weeks.put(Long.valueOf(id), new Week(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

				}
				rs.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			day++;
		}
		return weeks;
	}
	

	public static Map<Long, Week> getWeekForName(String name)
	{
		String name_query = null;
		String string = "2015-08-05";
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_day_str = null;		
		String month_str = null;
		String rowKey = null;
		String day_str = null;
		LocalDate localdate = new LocalDate(string);	
		LocalDate monday = localdate.withDayOfWeek(DateTimeConstants.MONDAY);
		int month = monday.getMonthOfYear();
		int year = monday.getYear();	
		int day = monday.getDayOfMonth();
		PrefixFilter filter;
		String year_str = String.valueOf(year);
		Table table;
		ResultScanner rs;
		
		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		}

		weeks.clear();
		
		for (int d =0; d<7; d++) {
			if (day < 10) {
				day_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
			}

			name_query = name + " " + year_str + "-" + month_str + "-" + day_str;
		    filter = new PrefixFilter(Bytes.toBytes(name_query));
			scan.setFilter(filter);
			
			try
			{
				table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
				rs = table.getScanner(scan);
				
				for (Result res : rs)
				{
					id += 1L;

					rowKey = Bytes.toString(res.getRow());
					String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();					
					
					date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
					Date date_day = format.parse(date_str);
					date_day_str    = df.format(date_day);
					
					String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
					BigDecimal num = new BigDecimal(value);
					num = num.setScale(2, BigDecimal.ROUND_CEILING);
					String valueWithNoExponents = num.toPlainString();

					weeks.put(Long.valueOf(id), new Week(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

				}
				rs.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			day++;
		}
		return weeks;
	}


	public static Map<Long, Month> getAllMonths()
	{
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_month_str = null;
		String rowKey = null;
		Table table;
		ResultScanner rs;

		months.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();				

				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_month = format.parse(date_str);
				date_month_str    = df.format(date_month);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				months.put(Long.valueOf(id), new Month(id, rowKey_name, "not avaiable", date_month_str, valueWithNoExponents ));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return months;
	}

	
	public static Map<Long, Month> getMonthForNameAndMonth(String name, String date)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_month_str = null;
		String rowKey = null;
		PrefixFilter filter;
		Table table;
		ResultScanner rs;

		name_query = name + " " + date;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		months.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());								
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_month = format.parse(date_str);
				date_month_str    = df.format(date_month);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				months.put(Long.valueOf(id), new Month(id, rowKey_name, "not avaiable", date_month_str, valueWithNoExponents ));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return months;
	}

	
	public static Map<Long, Month> getMonthsForName(String name)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_month_str = null;
		String month_str = null;
		String rowKey = null;
		LocalDateTime localdate = new LocalDateTime();		
		int month = localdate .getMonthOfYear();
		int year = localdate .getYear();
		String year_str = String.valueOf(year);
		PrefixFilter filter;
		Table table;
		ResultScanner rs;
		
		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		}
		
		month_str = "08";
		year_str = "2015";

		name_query = name + " " + year_str + "-" + month_str;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		months.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;

				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();			

				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_month = format.parse(date_str);
				date_month_str    = df.format(date_month);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));	
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				months.put(Long.valueOf(id), new Month(id, rowKey_name, "not avaiable", date_month_str, valueWithNoExponents ));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return months;
	}


	public static Map<Long, Year> getAllYear()
	{
		String string = "2013-08-25";
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		String date_year_str = null;		
		Scan scan = new Scan();
		String rowKey = null;
		LocalDate localdate = new LocalDate(string);	
		int year = localdate .getYear();	
		Table table;
		ResultScanner rs;
		

		years.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();				
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_year = format.parse(date_str);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date_year);
				int year_result = cal.get(Calendar.YEAR);
				date_year_str  = df.format(date_year);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				if(year == year_result)
					years.put(Long.valueOf(id), new Year(id, rowKey_name,  "not avaiable" , date_year_str, valueWithNoExponents));
			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return years;
	}

	public static Map<Long, Year> getYearForNameAndMonth(String name, String date)
	{
		String name_query = null;
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		String date_year_str = null;	
		String rowKey = null;
		Scan scan = new Scan();
		PrefixFilter filter;
		Table table;
		ResultScanner rs;		

		name_query = name + " " + date;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		years.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();				
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_year = format.parse(date_str);
				date_year_str  = df.format(date_year);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				years.put(Long.valueOf(id), new Year(id, rowKey_name,  "not avaiable" , date_year_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return years;
	}

	public static Map<Long, Year> getYearForName(String name)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;	
		String rowKey = null;
		LocalDateTime localdate = new LocalDateTime();		
		int year = localdate .getYear();
		String year_str = String.valueOf(year);
		PrefixFilter filter;
		Table table;
		ResultScanner rs;

		year_str = "2013";

		name_query = name + " " + year_str;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		years.clear();
		
		try
		{
			table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			rs = table.getScanner(scan);
			
			for (Result res : rs)
			{
				id += 1L;

				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
							
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_year = format.parse(date_str);
				date_year_str  = df.format(date_year);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				years.put(Long.valueOf(id), new Year(id, rowKey_name,  "not avaiable" , date_year_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return years;
	}



	public static Map<Long, Overall> getAllOverall()
	{
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		String date_year_str = null;		
		Scan scan = new Scan();
		String rowKey = null;

		overalls.clear();
		
		try
		{
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			ResultScanner rs = table.getScanner(scan);
			for (Result res : rs)
			{
				id += 1L;
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();				
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_year = format.parse(date_str);
				date_year_str  = df.format(date_year);

				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				overalls.put(Long.valueOf(id), new Overall(id, rowKey_name,  "not avaiable" , date_year_str, valueWithNoExponents));
			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return overalls;
	}

	public static Map<Long, Overall> getOverallForNameAndDate(String name, String date)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;	
		String rowKey = null;
		PrefixFilter filter;

		name_query = name + " " + date;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		overalls.clear();
		
		try
		{
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			ResultScanner rs = table.getScanner(scan);
			for (Result res : rs)
			{
				id += 1L;				
				
				rowKey = Bytes.toString(res.getRow());
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_year = format.parse(date_str);
				date_year_str  = df.format(date_year);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				overalls.put(Long.valueOf(id), new Overall(id, rowKey_name,  "not avaiable" , date_year_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return overalls;
	}

	public static Map<Long, Overall> getOverallForName(String name)
	{
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;	
		String rowKey = null;
		PrefixFilter filter;

		name_query = name;;
		filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		
		overalls.clear();
		
		try
		{
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			ResultScanner rs = table.getScanner(scan);
			for (Result res : rs)
			{
				id += 1L;

				rowKey = Bytes.toString(res.getRow());				
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
				
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				Date date_year = format.parse(date_str);
				date_year_str  = df.format(date_year);
				
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));				
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				overalls.put(Long.valueOf(id), new Overall(id, rowKey_name,  "not avaiable" , date_year_str, valueWithNoExponents));

			}
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return overalls;
	}


	public static String getSubstringUntilFirstNumber(String source)
	{
		return source.split("(\\d{4}-\\d{2})")[0];
	}
}
