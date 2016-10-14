package streamer;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;


/**
 * @author nilstes
 */
public class ESWriter {

    private static final Logger log = Logger.getLogger(ESWriter.class.getName());

    private String indexName;
    private String typeName;
    private Map<String,String> indexParam;
    private RestClient client;
    
    public ESWriter(String indexName, String typeName) throws Exception {
        log.info("ESWriter: inititalizing...");
        
        this.indexName = indexName;
        this.typeName = typeName;   
               
        connect();
        checkIndex();
        
        log.info("ESWriter: ok");
    }

    private void connect() throws UnknownHostException, Exception {
        Properties config = getConfig();
        String host = config.getProperty("host", "localhost");
        String port = config.getProperty("port", "9200");
        String pipeline = config.getProperty("pipeline", "");
        indexParam = !pipeline.equals("")?Collections.singletonMap("pipeline", pipeline):Collections.emptyMap();
        log.info("ESWriter: connecting to " + host + " on port " + port);
        HttpHost httpHost = new HttpHost(host, Integer.parseInt(port));
        client = RestClient.builder(httpHost).build();
    }

    private void checkIndex() throws IOException {
        try {
            client.performRequest("GET", "/" + indexName, Collections.emptyMap());
        } catch(ResponseException e) {
            client.performRequest("PUT", "/" + indexName, Collections.emptyMap());
            log.log(Level.INFO, "ESWriter: created new index {0}", indexName);
        }
    }
    
    public void addStatus(Message message) {
        try {
            HttpEntity entity = new NStringEntity(
                new Gson().toJson(message), ContentType.APPLICATION_JSON);
            client.performRequest(
                "POST",
                "/" + indexName + "/" + typeName,
                indexParam,
                entity);
            log.log(Level.INFO, "Added message to ElasticSearch: user={0}, message={1}", new Object[]{message.getUser(), message.getMessage()});
        } catch(Exception e) {
            log.log(Level.INFO, "Failed to add message to ElasticSearch: user={0}, message={1}, error={2}", new Object[]{message.getUser(), message.getMessage(), e.getMessage()});
        }
    }

    private Properties getConfig() throws Exception {
        File file = new File("es.properties");
        if(!file.exists()) {
            return new Properties();
        }
        Properties config = new Properties();
        config.load(new FileInputStream(file));
        return config;
    }
}
