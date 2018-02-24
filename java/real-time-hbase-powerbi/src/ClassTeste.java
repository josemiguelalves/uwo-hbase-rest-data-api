import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.LocalDateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


public class ClassTeste {
	
	public static final MediaType JSON 	= MediaType.parse("application/json; charset=utf-8");
	public static String myTime = "2017-08-01 17:46:00";
	public static float value =(float) 13.00;

	public void readData_fromFile()  {

		try {

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date d = df.parse(myTime); 
			Calendar cal = Calendar.getInstance();
			cal.setTime(d);
			cal.add(Calendar.SECOND, 30);	
			Client client = ClientBuilder.newClient();
			String url = "http://129.100.174.152:8080/hbase_data_api/webapi/year";
			javax.ws.rs.core.Response  response = client.target(url).request().get();
			String location = "C:\\Users\\jose-miguel\\Desktop\\year1.json";
			FileOutputStream out = new FileOutputStream(location);
			InputStream is = (InputStream)response.getEntity();
			int len = 0;
			byte[] buffer = new byte[4096];
			while((len = is.read(buffer)) != -1) {
				out.write(buffer, 0, len);
			}
			out.flush();
			out.close();
			is.close();

		}
		catch (Exception e) {
			System.out.println("\nError while calling Crunchify REST Service");
			System.out.println(e);
		}						
	}

	public void writeData_fromFile()  {

		@SuppressWarnings("unused")
		String string = "";
		try {

			// Step1: Let's 1st read file from fileSystem
			// Change CrunchifyJSON.txt path here
			InputStream crunchifyInputStream = new FileInputStream("C:\\Users\\jose-miguel\\Desktop\\year.json");
			InputStreamReader crunchifyReader = new InputStreamReader(crunchifyInputStream);
			BufferedReader br = new BufferedReader(crunchifyReader);
			String line;
			while ((line = br.readLine()) != null) {
				string += line + "\n";
			}

			JSONArray jsonObject = new JSONArray(new JSONTokener(new FileReader("C:\\Users\\jose-miguel\\Desktop\\year.json")));
			System.out.println(jsonObject);
			// Step2: Now pass JSON File Data to REST Service
			//String teste = "[{\"datatype\":\"not avaiable\",\"date\":\"2017-07-28 09:01:02:01\",\"id\":1,\"location\":\"1st Fl Light-Total kWh\",\"value\":\"86.00\"}]";
			try {

				RequestBody body = RequestBody.create(JSON, jsonObject.toString());
				OkHttpClient client1 = new OkHttpClient();

				Request request1 = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/5950779c-6ba4-4a4f-8dbb-bdaadedac52a/rows?key=EHHdRFUutJJe3OOpwLY%2B%2BPSSA7Dt%2BLhkMRKNjOlT%2BOB1ceJtVKJuaUWzM47Q9crbMnSYPfvrSgDqX4froCI45g%3D%3D")
						.post(body)
						.addHeader("content-type", "application/x-www-form-urlencoded")
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "7fdbdf2d-f0d0-3a35-0516-5cd879784541")
						.build();

				Response response1 = client1.newCall(request1).execute();
				response1.body().close();

			} catch (Exception e) {
				System.out.println("\nError while calling Crunchify REST Service");
				System.out.println(e);
			}

			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void writeData_Simulate ()  {

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (int i = 0; i < 1000; i++)  {

			try {

				// Wind Direction	
				Date d = df.parse(myTime); 
				Calendar cal = Calendar.getInstance();
				cal.setTime(d);
				cal.add(Calendar.SECOND, 1);
				String newTime = df.format(cal.getTime());
				myTime = newTime;
				double lower = 1;
				double upper = 360;
				double result = Math.random() * (upper - lower) + lower;
				String value_api =Double.toString(result);
				LinkedHashMap<String, String> jsonOrderedMap = new LinkedHashMap<String, String>();
				// jsonOrderedMap.put("datatype","not avaiable");
				jsonOrderedMap.put("date", newTime);
				jsonOrderedMap.put("name", "Wind Direction");
				//	jsonOrderedMap.put("id", "1");
				jsonOrderedMap.put("value", value_api );
				JSONObject obj = new JSONObject(jsonOrderedMap);
				JSONArray jsonArray = new JSONArray(Arrays.asList(obj));

				//System.out.println(jsonArray.toString());
				RequestBody body = RequestBody.create(JSON, jsonArray.toString());
				OkHttpClient client1 = new OkHttpClient();

				Request request = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/c1bef47c-ee4c-40d3-aed2-8a1005d05e38/rows?key=VFWxwgOLPN0ieQEOOf%2FaTlo5T%2Fe0%2FBucUA3TQLNR35DkOZdzu4nMr%2FaOfOWvymVmBBaj%2Bg4Y16bnVeFUFgJl%2FA%3D%3D")
						.post(body)
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "624627cb-b4f4-7dc4-772e-4da44aa68a9b")
						.build();

				Response response1 = client1.newCall(request).execute();
				response1.body().close();

				// Outside Humidity	
				lower = 1;
				upper = 100;
				d = df.parse(newTime); 
				cal = Calendar.getInstance();
				cal.setTime(d);
				cal.add(Calendar.SECOND, 1);
				newTime = df.format(cal.getTime());
				result = Math.random() * (upper - lower) + lower;
				jsonOrderedMap = new LinkedHashMap<String, String>();
				value_api =Double.toString(result);
				// jsonOrderedMap.put("datatype","not avaiable");
				jsonOrderedMap.put("date", newTime);
				jsonOrderedMap.put("name", "Outside Humidity");
				//jsonOrderedMap.put("id", "1");
				jsonOrderedMap.put("value", value_api );
				obj = new JSONObject(jsonOrderedMap);
				jsonArray = new JSONArray(Arrays.asList(obj));
				//System.out.println(jsonArray.toString());
				body = RequestBody.create(JSON, jsonArray.toString());	

				request = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/11711811-357a-404d-b6ca-b279f58c4cf7/rows?key=VyUpcNYzpMz%2B8Mg%2FVSEfopDH3A3guzwrpFduRgb4t%2BxFP8SE1SAhPsobJ3mu4mF88QWiEox56hHeYNrxb%2BV9qA%3D%3D")
						.post(body)
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "9f3b3e46-706d-b599-1000-d28de4c9080c")
						.build();

				response1 = client1.newCall(request).execute();
				response1.body().close();

				// Main Electricity Demand		
				lower = 10000;
				upper = 30000;
				d = df.parse(newTime); 
				cal = Calendar.getInstance();
				cal.setTime(d);
				cal.add(Calendar.SECOND, 1);
				newTime = df.format(cal.getTime());
				result = Math.random() * (upper - lower) + lower;
				jsonOrderedMap = new LinkedHashMap<String, String>();
				value_api =Double.toString(result);

				// jsonOrderedMap.put("datatype","not avaiable");
				jsonOrderedMap.put("date", newTime);
				jsonOrderedMap.put("name", "Main Electricity Demand");
				//jsonOrderedMap.put("id", "1");
				jsonOrderedMap.put("value", value_api );

				obj = new JSONObject(jsonOrderedMap);
				jsonArray = new JSONArray(Arrays.asList(obj));
				//System.out.println(jsonArray.toString());
				body = RequestBody.create(JSON, jsonArray.toString());	

				request = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/bdd99a3e-6a58-4710-888d-1efaac82fba9/rows?key=ONEg%2F2kBYJSTRpp5HD9DQegrVVXFj22YJjNk%2FRu3wply4hUizvHQTulpLT7PtSk9V%2FaQ6rJUJM6MHQ2gIzadBQ%3D%3D")
						.post(body)
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "53b0e42d-386b-0b23-4219-25947b657b3e")
						.build();

				response1 = client1.newCall(request).execute();
				response1.body().close();

				Thread.sleep(1000);

			} catch (Exception e) {
				System.out.println("\nError while calling Crunchify REST Service");
				System.out.println(e);
			}
		}
	}


	public static void writeData_fromAPI()  {

		while(true) {

			try {

				// Wind Direction
				Client client = ClientBuilder.newClient();
				String url = "http://129.100.174.152:8080/hbase_data_api/webapi/stream?name=Wind%20Direction";
				javax.ws.rs.core.Response  response = client.target(url).request().get();

				InputStream is = (InputStream)response.getEntity();

				String result = IOUtils.toString(is, StandardCharsets.UTF_8);
				RequestBody body = RequestBody.create(JSON, result);
				OkHttpClient client_post = new OkHttpClient();

				Request request = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/c1bef47c-ee4c-40d3-aed2-8a1005d05e38/rows?key=VFWxwgOLPN0ieQEOOf%2FaTlo5T%2Fe0%2FBucUA3TQLNR35DkOZdzu4nMr%2FaOfOWvymVmBBaj%2Bg4Y16bnVeFUFgJl%2FA%3D%3D")
						.post(body)
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "624627cb-b4f4-7dc4-772e-4da44aa68a9b")
						.build();

				Response response_post = client_post.newCall(request).execute();				
				response_post.body().close();

				// Outside Humidity
				client = ClientBuilder.newClient();
				url = "http://129.100.174.152:8080/hbase_data_api/webapi/stream?name=Outside%20Humidity";
				response = client.target(url).request().get();
				is = (InputStream)response.getEntity();
				result = IOUtils.toString(is, StandardCharsets.UTF_8);
				body  = RequestBody.create(JSON, result);
				client_post = new OkHttpClient();

				request = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/11711811-357a-404d-b6ca-b279f58c4cf7/rows?key=VyUpcNYzpMz%2B8Mg%2FVSEfopDH3A3guzwrpFduRgb4t%2BxFP8SE1SAhPsobJ3mu4mF88QWiEox56hHeYNrxb%2BV9qA%3D%3D")
						.post(body)
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "9f3b3e46-706d-b599-1000-d28de4c9080c")
						.build();

				response_post= client_post.newCall(request).execute();
				//	System.out.println(response_post.code());
				response_post.body().close();


				// Main nElectricity Demand				
				client = ClientBuilder.newClient();
				url = "http://129.100.174.152:8080/hbase_data_api/webapi/stream?name=Main%20Electricity%20Demand";
				response = client.target(url).request().get();
				is = (InputStream)response.getEntity();
				result = IOUtils.toString(is, StandardCharsets.UTF_8);
				body  = RequestBody.create(JSON, result);
				client_post = new OkHttpClient();

				request = new Request.Builder()
						.url("https://api.powerbi.com/beta/a7ba2ea9-be19-473e-bb09-118103e0d7bb/datasets/bdd99a3e-6a58-4710-888d-1efaac82fba9/rows?key=ONEg%2F2kBYJSTRpp5HD9DQegrVVXFj22YJjNk%2FRu3wply4hUizvHQTulpLT7PtSk9V%2FaQ6rJUJM6MHQ2gIzadBQ%3D%3D")
						.post(body)
						.addHeader("cache-control", "no-cache")
						.addHeader("postman-token", "53b0e42d-386b-0b23-4219-25947b657b3e")
						.build();

				response_post= client_post.newCall(request).execute();
				//	System.out.println(response_post.code());
				response_post.body().close();

				Thread.sleep(1000);

			} catch (Exception e) {
				System.out.println("\nError while calling Crunchify REST Service");
				System.out.println(e);
			}
		}
	}
 
	public static void test_date ()   {
//		String string = "2017-08-25";
//		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
//		DateTime dt = formatter.parseDateTime(string);		
//		LocalDate now = new LocalDate();
//		DateTime monday = dt.withDayOfWeek(DateTimeConstants.MONDAY);
//		System.out.println(monday);
//		
//		int day = monday.getDayOfMonth();
//		int month = monday.getMonthOfYear();
//		int year = monday.getYear();
//		
//		System.out.println(monday+" "+day+" "+month+" "+year);
//		LocalDate myDate = new LocalDate("2010-04-28");
		
		LocalDateTime now = new LocalDateTime();
		LocalDateTime monday = now.withDayOfWeek(DateTimeConstants.MONDAY);
		System.out.println(monday);
		
	}
	
//
//	public static void main(String[] args) throws Exception {
//
//		//writeData_Simulate();
//		//writeData_fromAPI();
//		//test_date();
//
//	}
	

}
