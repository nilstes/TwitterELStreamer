package streamer;

import com.google.gson.Gson;
import com.ning.http.client.AsyncHttpClient;
import de.otto.flummi.Flummi;
import de.otto.flummi.request.GsonHelper;
import java.io.File;
import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author nilstes
 */
public class ESWriter {

    private static final Logger log = Logger.getLogger(ESWriter.class.getName());

    private String indexName;
    private String typeName;
    private Flummi client;
    
    public ESWriter(String indexName, String typeName) throws Exception {
        log.info("ESWriter: inititalizing...");
        
        this.indexName = indexName;
        this.typeName = typeName;   
               
        connect();
        checkIndex(indexName);
        
        log.info("ESWriter: ok");
    }

    private void connect() throws UnknownHostException, Exception {
        Properties config = getConfig();
        String url = config.getProperty("url", "http://localhost:9200");
        
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
        client = new Flummi(asyncHttpClient, url);
    }

    private void checkIndex(String indexName1) {
        boolean exists = client.admin().indices().prepareExists(indexName1).execute().booleanValue();
        if (!exists) {
            client.admin().indices().prepareCreate(indexName1).execute();
            log.log(Level.INFO, "ESWriter: created new index {0}", indexName1);
        }
    }
    
    public void addStatus(Date createdAt, String user, String text) {
        try {
            client.prepareIndex()
                    .setIndexName(indexName)
                    .setDocumentType(typeName)
                    .setSource(GsonHelper.object(
                            "user", user,
                            "createdAt", new Gson().toJson(createdAt),
                            "message", text))                     
                    .execute();
            log.log(Level.INFO, "Added message to ElasticSearch: user={0}, message={1}", new Object[]{user, text});
        } catch(Exception e) {
            log.log(Level.INFO, "Failed to add message to ElasticSearch: user={0}, message={1}, error={2}", new Object[]{user, text, e.getMessage()});
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
