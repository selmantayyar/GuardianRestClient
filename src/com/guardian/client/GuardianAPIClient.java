package com.guardian.client;

import com.couchbase.client.CouchbaseClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/* This class retrieves articles list as json format from the guardian open data platform
 * and persists them into couchbase database configuration of which is in couchdbconf.properties file.
 * API_URL is your guy if you want to get more customized data.Currently,it fetches records randomly starting from 2012 till today.
 * 
 * */
public class GuardianAPIClient {
	   
	public final static String COUCHBASE_URIS = "couchbase.uri.list";
	public final static String COUCHBASE_BUCKET = "couchbase.bucket";
	public final static String COUCHBASE_PASSWORD = "couchbase.password";
    private List<URI> couchbaseServerUris = new ArrayList<URI>();
	private String couchbaseBucket = "";
	private String couchbasePassword = "";
	//private static String API_URL="http://content.guardianapis.com/search?format=json&tag=football/premierleague&from-date=2014-01-01&show-fields=headline,trailText";
	//see for more info about parameters. http://www.theguardian.com/open-platform/content-api-content-search-reference-guide?guni=Keyword:news-grid%20main-3%20Trailblock:Pickable%20with%20editable%20override:Position1:sublinks
	private static String API_URL="http://content.guardianapis.com/search?format=json&from-date=2012-01-01&show-fields=headline,trailText&order-by=relevance";
	private static String PAGE_NUM="&page=";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			GuardianAPIClient apiClient=new GuardianAPIClient();
			apiClient.retrieveDBProps();   
			
			apiClient.getNewsAndInsertIntoCouchDB();
	 
		  } catch (Exception e) {
	 
			e.printStackTrace();
	 
		  }
	 
		}

	public  void getNewsAndInsertIntoCouchDB() throws JSONException, IOException {
		Client client = Client.create();
 
		WebResource webResource = client
		   .resource(API_URL);
 
		ClientResponse response = webResource.accept("application/json")
		           .get(ClientResponse.class);
 
		if (response.getStatus() != 200) {
		   throw new RuntimeException("Failed : HTTP error code : "
			+ response.getStatus());
		}
 
		String output = response.getEntity(String.class);
		
		JSONObject outputJSON = new JSONObject(output);
		
		JSONObject newsJSON = outputJSON.getJSONObject("response");
		JSONArray newsArray = newsJSON.getJSONArray("results");
		Integer pageNumber=newsJSON.getInt("pages");
		System.out.println("There are  "+pageNumber+" of pages");
		Integer pageNumberLimit=pageNumber<=100?pageNumber:100;

	
		CouchbaseClient couchbaseClient=null;
		try {
			couchbaseClient = new CouchbaseClient( couchbaseServerUris , couchbaseBucket , couchbasePassword );
		    } catch (Exception e) {
		      System.err.println("Error connecting to Couchbase: " 
		        + e.getMessage());
		      System.exit(0);
		    }
		    
	    for (int i = 0; i < newsArray.length(); i++) {
		        	JSONObject news=newsArray.getJSONObject(i);
		        	//we have to escape slash,gives trouble later on angular js
		        	String idNoSlash=news.getString("id").replace('/', '_');
		        	news.put("id", idNoSlash);
		        	couchbaseClient.add(idNoSlash, 0, news.toString());
		        	System.out.println(news);
		    		System.out.print("  inserted into CB");
				}

		    
		for (int k = 2; k <= pageNumberLimit; k++) {
			
			webResource = client
			   .resource(API_URL+PAGE_NUM+k);
	 
			 response = webResource.accept("application/json")
			           .get(ClientResponse.class);
	 
			if (response.getStatus() != 200) {
			   throw new RuntimeException("Failed : HTTP error code : "
				+ response.getStatus());
			}
	 
			 output = response.getEntity(String.class);
			 outputJSON = new JSONObject(output);
			 newsJSON = outputJSON.getJSONObject("response");
			 newsArray = newsJSON.getJSONArray("results");
			 
			 for (int j = 0; j < newsArray.length(); j++) {
		        	JSONObject news=newsArray.getJSONObject(j);
		        	//we have to escape slash,gives trouble later on angular js
		        	String idNoSlash=news.getString("id").replace('/', '_');
		        	news.put("id", idNoSlash);
		        	couchbaseClient.add(idNoSlash, 0, news.toString());
		        	System.out.println(news);
		    		System.out.print("item "+j+" in page "+k+"  inserted into CB in the loop");
				}
		}
		System.out.println(" DONE");
		couchbaseClient.shutdown();
		
		
	
	}

	private  void retrieveDBProps() throws URISyntaxException, IOException {
		Properties props = new Properties();
		InputStream input = null;
		input = GuardianAPIClient.class.getClassLoader().getResourceAsStream("couchdbconf.properties");
		props.load(input);
		couchbaseServerUris.add(URI.create(props.getProperty(COUCHBASE_URIS)));
		couchbaseBucket=props.getProperty(COUCHBASE_BUCKET);
		couchbasePassword=props.getProperty(COUCHBASE_PASSWORD);
	}

	}

