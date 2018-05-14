        package com.powersmiths.hbase.connection;

        import java.io.IOException;


        import com.google.common.base.Stopwatch;
        import com.mongodb.client.FindIterable;
        import com.powersmiths.DAO.RealtimeDAO;
        import com.powersmiths.data.Realtime;
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
        import java.util.*;


        import com.mongodb.MongoClient;
        import com.mongodb.MongoClientURI;
        import com.mongodb.ServerAddress;

        import com.mongodb.client.MongoDatabase;
        import com.mongodb.client.MongoCollection;

        import org.bson.Document;
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
            private static final String columnName = "data_sumary";
            private static final String columnValue = "value";

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


                try {

                    conn = ConnectionFactory.createConnection(config);
                    System.out.println("Master info port: " + conn.getAdmin().getConfiguration().toString());
                    System.out.println("GetConfiguration: " + conn.getConfiguration().toString());
                } catch (IOException ex) {
                    System.out.println("IOException : " + ex.getMessage());
                } catch (NoClassDefFoundError ex) {
                    System.out.println("Error : " + ex.getMessage());
                }


                long id = 0L;
                String date_str = null;
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);

                String date_day_str = null;

                try {
                    table = conn.getTable(TableName.valueOf("powersmiths:readings_hour"));
                    rs = table.getScanner(scan);

                    for (Result res : rs) {
                        id += 1L;

                        rowKey = Bytes.toString(res.getRow());
                        String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

                        date_str = rowKey.substring(rowKey.length() - 13);
                        Date date_day = format.parse(date_str);
                        date_day_str = df.format(date_day);

                        String value = Bytes.toString(res.getValue(Bytes.toBytes("data_summary"), Bytes.toBytes("value")));
                        BigDecimal num = new BigDecimal(value);
                        num = num.setScale(2, BigDecimal.ROUND_CEILING);
                        String valueWithNoExponents = num.toPlainString();

                        // days.put(Long.valueOf(id), new Day(id, rowKey_name, "not avaiable", date_day_str, valueWithNoExponents));

                        //
                        //
                        //				System.out.println("value: " + name);

                        System.out.println("rowKey_name: " + rowKey_name + " date_day_str: " + date_day_str + " value: " + value);

                    }
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }


            private static String getSubstringUntilFirstNumber(String source) {
                return source.split("(\\d{4}-\\d{2})")[0];
            }


            public static void MongoConnectionDay() {

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
                String value;


                FindIterable<Document> results = collection.find();

                try {



                    for (Document doc : results) {
                        rowKey = doc.getString("data_sumary");
                        value = doc.getString("value");

                        String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

                        date_str = rowKey.substring(rowKey.length() - 13);
                        Date date_day = format.parse(date_str);
                        date_day_str  = df.format(date_day);



                        BigDecimal num = new BigDecimal(value);
                        num = num.setScale(2, BigDecimal.ROUND_CEILING);
                        String valueWithNoExponents = num.toPlainString();



                     System.out.println(rowKey+"   "+rowKey_name+"   "+date_day_str+"  "+valueWithNoExponents);



                    }
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


            public static void MongoConnectionDayNameDate() {


//                String date = null;
//                String name = null;
                MongoClient mongoClient = new MongoClient("129.100.174.152", 30021);
                MongoDatabase database = mongoClient.getDatabase("powersmiths");
                MongoCollection<Document> collection = database.getCollection("readings_hour");
                long id = 0L;
                String date_str = null;
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ENGLISH);
                String date_day_str = null;
                String rowKey = null;
                String value;
                String name_query;

//                name_query = name + " " + date;
                name_query = "WindDirection2018-01-03";



                FindIterable<Document> results = collection.find(Filters.regex("data_sumary", name_query));

                try {


                    for (Document doc : results) {
                        rowKey = doc.getString("data_sumary");
                        value = doc.getString("value");

                        String rowKey_name = getSubstringUntilFirstNumber(rowKey).trim();

                        date_str = rowKey.substring(rowKey.length() - 13);
                        Date date_day = format.parse(date_str);
                        date_day_str = df.format(date_day);


                        BigDecimal num = new BigDecimal(value);
                        num = num.setScale(2, BigDecimal.ROUND_CEILING);
                        String valueWithNoExponents = num.toPlainString();
                        System.out.println(rowKey + "   " + rowKey_name + "   " + date_day_str + "  " + valueWithNoExponents);
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }


            }


            public static void scan() {

                String date = null;
                String name = null;
                MongoClient mongoClient = new MongoClient("129.100.174.152", 30021);
                MongoDatabase database = mongoClient.getDatabase("powersmiths");
                MongoCollection<Document> collection = database.getCollection("readings_hour");

            float val = 0;
           FindIterable<Document> results = null;




        try

            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.start();

                results = collection.find();
                stopwatch.stop();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("read;" + timestamp + ";" + String.valueOf(stopwatch.elapsedMillis()));

            }
        catch(
            Exception e)

            {
                e.printStackTrace();
            }

        if(results !=null)

            {
                String value = String.valueOf(collection.count());
                val = Float.valueOf(value);
            }

        System.out.print(val);

        }

            public static void insertMany()  {


                MongoClient mongoClient = new MongoClient( "129.100.174.152" , 30021 );

                MongoDatabase database = mongoClient.getDatabase("powersmiths");
                MongoCollection<Document> collection = database.getCollection("readings_hour");



                List<Document> documents = new ArrayList<Document>();
                for (int i = 0; i < 10; i++) {
                    documents.add(new Document("data_sumary", "WindDirection2018-01-0"+i+" 01").append( "value", String.valueOf(Math.random()*100)));
                }


                Stopwatch stopwatch = new Stopwatch();
                stopwatch.start();
                collection.insertMany(documents);

                stopwatch.stop();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                System.out.println("write;" + timestamp + ";" + String.valueOf(stopwatch.elapsedMillis()));

            }


        public static void insert(Realtime record)  {


            MongoClient mongoClient = new MongoClient( "129.100.174.152" , 30021 );

            MongoDatabase database = mongoClient.getDatabase("powersmiths");
            MongoCollection<Document> collection = database.getCollection("readings_hour");

            String skey  = record.getKey();
            String svalue = String.valueOf(record.getValue());

            Document doc = new Document(columnName, skey)
                    .append(columnValue, svalue);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();
            collection.insertOne(doc);
            stopwatch.stop();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("write;" + timestamp + ";" + String.valueOf(stopwatch.elapsedMillis()));

            }


            @SuppressWarnings("deprecation")
            public static void main(String[] args) throws ServiceException, MasterNotRunningException, ZooKeeperConnectionException, IOException {

//             HbaseConnection();
 //            MongoConnectionDay();
//            MongoConnectionDayNameDate();
           scan();


                Realtime test_record = new Realtime();
                test_record.setKey("WindDirection2018-01-20 22");
                float test = (float)22.45;
//                test_record.setValue(test);
//                insert(test_record);

 //               insertMany();
   //             MongoConnectionDay();
            }

        }




