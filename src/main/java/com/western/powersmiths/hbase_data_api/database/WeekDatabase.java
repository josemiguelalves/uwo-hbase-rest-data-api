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

public class WeekDatabase
{

	private static Map<Long, Week> weeks = new HashMap<>();
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

		weeks.clear();

		try
		{
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
			ResultScanner rs = table.getScanner(scan);

			for (Result res : rs)
			{
				id += 1L;
				rowKey = Bytes.toString(res.getRow());
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);

				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				Date date_day = format.parse(date_str);
				date_day_str  = df.format(date_day);
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

		System.out.println(date+" "+monday+" "+day);
		weeks.clear();
		String year_str = String.valueOf(year);
		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		} else
			month_str = String.valueOf(month);



		for (int d =0; d<7; d++) {

			if (day < 10) {
				day_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
			} else
				day_str =  String.valueOf(day);		

			name_query = name + " " + year_str + "-" + month_str + "-" + day_str;
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name_query));
			scan.setFilter(filter);


			System.out.println(name_query);

			try
			{
				Table table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
				ResultScanner rs = table.getScanner(scan);
				for (Result res : rs)
				{
					id += 1L;
					rowKey = Bytes.toString(res.getRow());
					date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);

					String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
					String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

					Date date_day = format.parse(date_str);
					date_day_str    = df.format(date_day);
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
		weeks.clear();
		String year_str = String.valueOf(year);
		if (month < 10) {
			month_str = String.format("%02d", new Object[] { Integer.valueOf(month) });
		}


		for (int d =0; d<7; d++) {
			if (day < 10) {
				day_str = String.format("%02d", new Object[] { Integer.valueOf(day) });
			}

			name_query = name + " " + year_str + "-" + month_str + "-" + day_str;
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name_query));
			scan.setFilter(filter);


			System.out.println(name_query);

			try
			{
				Table table = conn.getTable(TableName.valueOf("powersmiths:readings_day"));
				ResultScanner rs = table.getScanner(scan);
				for (Result res : rs)
				{
					id += 1L;

					rowKey = Bytes.toString(res.getRow());
					date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);

					String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
					String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

					Date date_day = format.parse(date_str);
					date_day_str    = df.format(date_day);
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



	public static String getSubstringUntilFirstNumber(String source)
	{
		return source.split("(\\d{4}-\\d{2})")[0];
	}
}

