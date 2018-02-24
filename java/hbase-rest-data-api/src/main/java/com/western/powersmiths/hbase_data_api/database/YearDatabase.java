package com.western.powersmiths.hbase_data_api.database;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
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

import com.western.powersmiths.hbase_data_api.model.Year;

public class YearDatabase {

	private static Map<Long, Year> years = new HashMap<>();
	private static Connection conn = getConnection();
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static Connection getConnection() {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "192.168.56.100");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.master", "192.168.56.100:16000");
		try {
			conn = ConnectionFactory.createConnection(config);
		} catch (IOException ex) {
			System.out.println("IOException : " + ex.getMessage());
		} catch (NoClassDefFoundError ex) {
			System.out.println("Error : " + ex.getMessage());
		}
		return conn;
	}

	public static Map<Long, Year> getAllYear() {
		String string = "2013-08-25";
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		String date_year_str = null;
		Scan scan = new Scan();
		String rowKey = null;

		LocalDate localdate = new LocalDate(string);
		int year = localdate.getYear();

		years.clear();
		try {
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			ResultScanner rs = table.getScanner(scan);
			for (Result res : rs) {
				id += 1L;
				rowKey = Bytes.toString(res.getRow());
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);
				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				Date date_year = format.parse(date_str);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date_year);
				int year_result = cal.get(Calendar.YEAR);

				date_year_str = df.format(date_year);

				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				if (year == year_result)
					years.put(Long.valueOf(id),
							new Year(id, rowKey_name, "not avaiable", date_year_str, valueWithNoExponents));
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return years;
	}

	public static Map<Long, Year> getYearForNameAndMonth(String name, String date) {
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;
		String rowKey = null;

		name_query = name + " " + date;
		years.clear();
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		try {
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			ResultScanner rs = table.getScanner(scan);
			for (Result res : rs) {
				id += 1L;
				rowKey = Bytes.toString(res.getRow());
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);

				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				Date date_year = format.parse(date_str);
				date_year_str = df.format(date_year);
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				years.put(Long.valueOf(id),
						new Year(id, rowKey_name, "not avaiable", date_year_str, valueWithNoExponents));

			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return years;
	}

	public static Map<Long, Year> getYearForName(String name) {
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;
		String rowKey = null;

		LocalDateTime localdate = new LocalDateTime();
		int year = localdate.getYear();
		String year_str = String.valueOf(year);

		year_str = "2013";

		name_query = name + " " + year_str;
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		years.clear();
		try {
			Table table = conn.getTable(TableName.valueOf("powersmiths:readings_month"));
			ResultScanner rs = table.getScanner(scan);
			for (Result res : rs) {
				id += 1L;

				rowKey = Bytes.toString(res.getRow());
				date_str = rowKey.substring(rowKey.lastIndexOf(' ') + 1);

				String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
				String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

				Date date_year = format.parse(date_str);
				date_year_str = df.format(date_year);
				BigDecimal num = new BigDecimal(value);
				num = num.setScale(2, BigDecimal.ROUND_CEILING);
				String valueWithNoExponents = num.toPlainString();

				years.put(Long.valueOf(id),
						new Year(id, rowKey_name, "not avaiable", date_year_str, valueWithNoExponents));

			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return years;
	}

	public static String getSubstringUntilFirstNumber(String source) {
		return source.split("(\\d{4}-\\d{2})")[0];
	}
}
