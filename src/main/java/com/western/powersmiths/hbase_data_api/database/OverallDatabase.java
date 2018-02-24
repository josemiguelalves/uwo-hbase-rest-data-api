package com.western.powersmiths.hbase_data_api.database;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

import com.western.powersmiths.hbase_data_api.model.Overall;

public class OverallDatabase {
	private static Map<Long, Overall> overalls = new HashMap<>();
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

	public static Map<Long, Overall> getAllOverall() {
		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		String date_year_str = null;
		Scan scan = new Scan();
		String rowKey = null;

		overalls.clear();
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

				overalls.put(Long.valueOf(id),
						new Overall(id, rowKey_name, "not avaiable", date_year_str, valueWithNoExponents));
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return overalls;
	}

	public static Map<Long, Overall> getOverallForNameAndDate(String name, String date) {
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;
		String rowKey = null;

		name_query = name + " " + date;
		overalls.clear();
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

				overalls.put(Long.valueOf(id),
						new Overall(id, rowKey_name, "not avaiable", date_year_str, valueWithNoExponents));

			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return overalls;
	}

	public static Map<Long, Overall> getOverallForName(String name) {
		String name_query = null;

		long id = 0L;
		String date_str = null;
		DateFormat format = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
		Scan scan = new Scan();
		String date_year_str = null;
		String rowKey = null;

		name_query = name;
		;
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes(name_query));
		scan.setFilter(filter);
		overalls.clear();
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

				overalls.put(Long.valueOf(id),
						new Overall(id, rowKey_name, "not avaiable", date_year_str, valueWithNoExponents));

			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return overalls;
	}

	public static String getSubstringUntilFirstNumber(String source) {
		return source.split("(\\d{4}-\\d{2})")[0];
	}
}
