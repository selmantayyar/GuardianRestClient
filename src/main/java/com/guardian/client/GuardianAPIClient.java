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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/* This class retrieves articles list as json format from the guardian open data platform
 * and persists them into couchbase database configuration of which is in couchdbconf.properties file.
 * API_URL is your guy if you want to get more customized data.Currently,it fetches records randomly starting from 2012 till today.
 * api-key in the URL is the key i have retrieved free from theguardian open platform.
 * 
 * */
public class GuardianAPIClient {

    private static Logger LOG = Logger.getLogger(GuardianAPIClient.class.getName());

    public final static String COUCHBASE_URIS = "couchbase.uri.list";
    public final static String COUCHBASE_BUCKET = "couchbase.bucket";
    public final static String COUCHBASE_PASSWORD = "couchbase.password";
    private static List<URI> couchbaseServerUris = new ArrayList<URI>();
    private static String couchbaseBucket = "";
    private static String couchbasePassword = "";

    private static String API_URL = "http://content.guardianapis"
            + ".com/search?api-key=d4135b57-a73d-4fca-aa62-ada8116b328e&format=json&from-date=2012-01-01&show-fields"
            + "=headline,trailText&order-by=relevance";

    private static String PAGE_NUM = "&page=";

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {

            retrieveDBProps();
            getNewsAndInsertIntoCouchDB();

        } catch (Exception e) {
            LOG.log(Level.SEVERE,"An error occured,application stopping.");
            System.exit(1);
        }

    }

    /*
      Method retrieving data from The Guardian Open Platform and Persisting it into Couchbase instance.
      Customize URL if you have different kinds of news,as you please.
      More info available here http://open-platform.theguardian.com/documentation/
     */
    public static void getNewsAndInsertIntoCouchDB() throws JSONException, IOException {
        Client client = Client.create();

        WebResource webResource = client.resource(API_URL);

        ClientResponse response = webResource.accept("application/json")
                .get(ClientResponse.class);

        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatus());
        }

        //First call to get how many pages of data we have.
        Integer pageNumber = getNumberOfPages(response);
        //Let's limit the number of pages
        Integer pageNumberLimit = pageNumber <= 25 ? pageNumber : 25;

        CouchbaseClient couchbaseClient = getCouchbaseClient();

        //Now we have the number of pages,we persist the data page by page.
        for (int k = 1; k <= pageNumberLimit; k++) {

            webResource = client.resource(API_URL + PAGE_NUM + k);

            response = webResource.accept("application/json").get(ClientResponse.class);

            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }

            String output = response.getEntity(String.class);
            JSONObject outputJSON = new JSONObject(output);
            JSONObject newsJSON = outputJSON.getJSONObject("response");
            JSONArray newsArray = newsJSON.getJSONArray("results");

            for (int j = 0; j < newsArray.length(); j++) {
                JSONObject news = persistIntoCB(newsArray, couchbaseClient, j);
                LOG.log(Level.INFO,news.get("webTitle").toString());
                LOG.log(Level.INFO,"item " + j + " in page " + k + "  inserted into CB in the loop");
            }
        }

        LOG.log(Level.INFO," Persisting News into Couchbase Completed");
        couchbaseClient.shutdown();


    }

    private static Integer getNumberOfPages(ClientResponse response) throws JSONException {
        String output = response.getEntity(String.class);

        JSONObject outputJSON = new JSONObject(output);

        JSONObject newsJSON = outputJSON.getJSONObject("response");
        JSONArray newsArray = newsJSON.getJSONArray("results");
        Integer pageNumber = newsJSON.getInt("pages");

        LOG.log(Level.INFO,"There are  " + pageNumber + " of pages");
        return pageNumber;
    }

    private static JSONObject persistIntoCB(JSONArray newsArray, CouchbaseClient couchbaseClient, int i)
            throws JSONException {
        JSONObject news = newsArray.getJSONObject(i);
        //we have to escape slash,gives trouble later on angular js
        String idNoSlash = news.getString("id").replace('/', '_');
        news.put("id", idNoSlash);
        couchbaseClient.add(idNoSlash, 0, news.toString());
        return news;
    }

    private static CouchbaseClient getCouchbaseClient() {
        CouchbaseClient couchbaseClient = null;
        try {
            couchbaseClient = new CouchbaseClient(couchbaseServerUris, couchbaseBucket, couchbasePassword);
        } catch (Exception e) {
            LOG.log(Level.SEVERE,"Error connecting to Couchbase,application stopping."+ e.getMessage());
            System.exit(1);
        }
        return couchbaseClient;
    }

    private static void retrieveDBProps() throws URISyntaxException, IOException {
        Properties props = new Properties();
        InputStream input = null;
        input = GuardianAPIClient.class.getClassLoader().getResourceAsStream("couchdbconf.properties");
        props.load(input);
        couchbaseServerUris.add(URI.create(props.getProperty(COUCHBASE_URIS)));
        couchbaseBucket = props.getProperty(COUCHBASE_BUCKET);
        couchbasePassword = props.getProperty(COUCHBASE_PASSWORD);
    }

}

