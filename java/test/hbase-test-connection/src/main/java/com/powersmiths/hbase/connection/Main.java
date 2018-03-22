        package com.powersmiths.hbase.connection;

        import java.io.IOException;


        import com.mongodb.client.FindIterable;
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




        import com.mongodb.MongoClient;
        import com.mongodb.MongoClientURI;
        import com.mongodb.ServerAddress;

        import com.mongodb.client.MongoDatabase;
        import com.mongodb.client.MongoCollection;

        import org.bson.Document;
        import java.util.Arrays;
        import com.mongodb.Block;

        import com.mongodb.client.MongoCursor;
        import static com.mongodb.client.model.Filters.*;
        import com.mongodb.client.result.DeleteResult;
        import static com.mongodb.client.model.Updates.*;
        import com.mongodb.client.result.UpdateResult;

        import com.mongodb.client.model.Indexes;
        import com.mongodb.client.model.Filters;
        import com.mongodb.client.model.Sorts;
        import com.mongodb.client.model.TextSearchOptions;
        import com.mongodb.client.model.Projections;
        import org.bson.Document;
        import com.mongodb.BasicDBObject;
        import org.bson.conversions.Bson;


        public class Main {

            private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                public static void HbaseConnection() {


                    Table table;
                    ResultScanner rs;
                    Scan scan = new Scan();
                    String rowKey = null;
                    Get theGet;
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    Connection conn = null;


                    Configuration config = HBaseConfiguration.create();
                    config.set("hbase.zookeeper.quorum", "129.100.174.152");
                    config.set("hbase.zookeeper.property.clientPort", "2181");
                    config.set("hbase.master", "129.100.174.152:16000");
                    config.set("hbase.client.retries.number", Integer.toString(3));
                    config.set("zookeeper.session.timeout", Integer.toString(60000));
                    config.set("zookeeper.recovery.retry", Integer.toString(3));
                    config.set("hbase.cluster.distributed", "true");
                    config.set("hbase.hbase.regionserver", "129.100.174.152:16023, 129.100.174.152:16022, 129.100.174.152:16021, 129.100.174.152:16029");



                    try
                    {

                        conn = ConnectionFactory.createConnection(config);
                        System.out.println("Master info port: "+conn.getAdmin().getConfiguration().toString());
                        System.out.println("GetConfiguration: "+conn.getConfiguration().toString());
                    }
                    catch (IOException ex)
                    {
                        System.out.println("IOException : " + ex.getMessage());
                    }
                    catch (NoClassDefFoundError ex)
                    {
                        System.out.println("Error : " + ex.getMessage());
                    }



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

                            // days.put(Long.valueOf(id), new Day(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

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




            public static void MongoConnection() {

                MongoClient mongoClient = new MongoClient( "129.100.174.152" , 30021 );
                MongoDatabase database = mongoClient.getDatabase("powersmiths");
                MongoCollection<Document> collection = database.getCollection("readings_hour");
                Bson nameRegexp = Filters.regex("data_sumary", "WindDirection 2018-01-011 01:01:01");
                long id = 0L;
                String date_str = null;
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);
                Scan scan = new Scan();
                String date_day_str = null;
                String rowKey = null;
                Double value;


//                MongoCursor<Document> cursor = collection.find().iterator();
//
                FindIterable<Document> results = collection.find();

                try {



                    for (Document doc : results) {
                        rowKey = doc.getString("data_sumary");
                        value = doc.getDouble("value");

                        String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

                        date_str = rowKey.substring(rowKey.length() - 13);
                        Date date_day = format.parse(date_str);
                        date_day_str  = df.format(date_day);


                        BigDecimal num = new BigDecimal(value);
                        num = num.setScale(2, BigDecimal.ROUND_CEILING);
                        String valueWithNoExponents = num.toPlainString();
                        System.out.println(rowKey+"   "+rowKey_name+"   "+date_day_str+"  "+valueWithNoExponents);
                    }



//
//                    while (cursor.hasNext()) {
//                        rowKey = cursor.next().getString("data_sumary");
//                        value = cursor.next().getDouble("value");
//
//                        String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();
//
//                        date_str = rowKey.substring(rowKey.length() - 13);
//                        Date date_day = format.parse(date_str);
//                        date_day_str  = df.format(date_day);
//
//
//                        BigDecimal num = new BigDecimal(value);
//                        num = num.setScale(2, BigDecimal.ROUND_CEILING);
//                        String valueWithNoExponents = num.toPlainString();



 //                       System.out.println(rowKey+"   "+rowKey_name+"   "+date_day_str+"  "+valueWithNoExponents);



                        //System.out.println(cursor.next().toJson());
 //                   }
                }

                catch (Exception e)
                {
                    e.printStackTrace();
                }
//
//                finally {
//                    cursor.close();
//                }

//                Block<Document> printBlock = new Block<Document>() {
//                    @Override
//                    public void apply(final Document document) {
//                        System.out.println(document.toJson());
//                    }
//                };
//
//                collection.createIndex(Indexes.text("data_sumary"));
////
//                collection.find(Filters.text("WindDirection 2018-01-011 01:01:01")).projection(Projections.metaTextScore("score"))
//                        .sort(Sorts.metaTextScore("score")).forEach(printBlock);

//
//               collection.find(Filters.text("WindDirection 2018-01-011 01:01:01"));
////
//                Bson nameRegexp = Filters.regex("data_sumary", "WindDirection 2018-01-011 01:01:01");
//                collection.find(nameRegexp).forEach(printBlock);


//
//                    DBCollection collection = db.getCollection(collectionName);
//                    BasicDBObject searchQuery = new BasicDBObject();
//                    searchQuery.put("data_summay", "WindDirection 2018-01-011 01:01:01");
//                    System.out.println("search Query ::" + searchQuery);
//                    DBCursor cursor = collection.find(searchQuery);

                }



            @SuppressWarnings("deprecation")
            public static void main(String[] args) throws ServiceException, MasterNotRunningException, ZooKeeperConnectionException, IOException {

//              HbaseConnection();
               MongoConnection();

            }

        }




