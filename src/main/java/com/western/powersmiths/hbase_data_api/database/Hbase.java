package com.western.powersmiths.hbase_data_api.database;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class Hbase {
	
	
	
	
	
	
	public static void Main() {
	
	Configuration config = HBaseConfiguration.create();
	config.set("hbase.zookeeper.quorum", "quickstart.cloudera");    
	config.set("hbase.zookeeper.property.clientPort", "2181");
	config.set("hbase.master", "quickstart.cloudera:60000");


	NamespaceDescriptor[] namespaces;
	TableName[] tables;
	HTableDescriptor htd1;


	try {


		Connection conn =ConnectionFactory.createConnection(config);


		Admin admin  = conn.getAdmin();

		System.out.println(admin.getMasterInfoPort());
		//createTables(admin, conn);
		//readCSV();

		tables = admin.listTableNames();
		for (TableName table: tables) {
			System.out.println("Table : " + table);
			htd1 = admin.getTableDescriptor(table);
			System.out.println(htd1);
		}

		System.out.println("Table enabled : " + admin.tableExists(TableName.valueOf("powersmiths", "readings")));

		namespaces = admin.listNamespaceDescriptors();
		for (NamespaceDescriptor name: namespaces) {
			System.out.println("Namespace : " + name);
		}

		//System.out.println(admin.listNamespaceDescriptors());
		conn.close();



	} catch (IOException ex) {
		System.out.println("IOException : " + ex.getMessage());          
	} catch (NoClassDefFoundError ex) {
		System.out.println("Error : " + ex.getMessage());          

	}
	
	}
	
	   protected static void createTables(Admin admin, Connection conn) throws IOException {
	        
		   
	        // Instantiating table descriptor class
	        TableName tableName = TableName.valueOf("powersmiths", "readings");
	        if ((admin.tableExists(tableName))) {
	            admin.disableTable(tableName);
	            admin.deleteTable(tableName);
	        }
	        
	        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
	        
	        // Adding column families to table descriptor
	        tableDescriptor.addFamily(new HColumnDescriptor("data_readings"));
	        
	        // Execute the table through admin
	        admin.createTable(tableDescriptor);
	        insertData(tableDescriptor, conn); 
	    }
	   
	   
	   
	   
	    protected static void insertData(HTableDescriptor table, Connection conn) throws IOException {

	        Table tbl = conn.getTable(table.getTableName());   

	        
	        CSVReader reader = new CSVReader(new FileReader("/home/jose/Desktop/PowerSmiths/13045.csv"));
	        String [] nextLine;
	        while ((nextLine = reader.readNext()) != null) {
	           Put put = new Put(Bytes.toBytes(nextLine[0]));
	           put.addColumn(Bytes.toBytes("data_readings"), Bytes.toBytes("DATAPOINT_USERNAME"),Bytes.toBytes(nextLine[1]));
	           put.addColumn(Bytes.toBytes("data_readings"), Bytes.toBytes("UTC_DATETIME"),Bytes.toBytes(nextLine[2]));
	           put.addColumn(Bytes.toBytes("data_readings"), Bytes.toBytes("LOCAL_DATETIME"),Bytes.toBytes(nextLine[3]));
	           put.addColumn(Bytes.toBytes("data_readings"), Bytes.toBytes("UTC_OFFSET"),Bytes.toBytes(nextLine[4]));
	           put.addColumn(Bytes.toBytes("data_readings"), Bytes.toBytes("READING_VALUE"),Bytes.toBytes(nextLine[5]));
	          
	           tbl.put(put);
	           // nextLine[] is an array of values from the line
	           //System.out.println(nextLine[0] + " " + nextLine[1] + " "  + nextLine[2] + "  " + nextLine[3] + " " + nextLine[4] + " " + nextLine[5]);
	        }
	     
	       }
	
	

}
