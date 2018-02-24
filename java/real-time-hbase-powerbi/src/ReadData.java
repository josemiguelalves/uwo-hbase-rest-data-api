
import java.io.FileOutputStream;
import java.io.InputStream;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

public class ReadData {

	// http://localhost:8080/RESTfulExample/json/product/get
	public static void main(String[] args) throws Exception {

		Client client = ClientBuilder.newClient();
		String url = "http://129.100.174.152:8080/hbase_data_api/webapi/year";
		Response response = client.target(url).request().get();
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

}