package streamer;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author nilstes
 */
public class ESWriter {

    private static final Logger log = Logger.getLogger(ESWriter.class.getName());

    private String indexName;
    private String typeName;
    private TransportClient client;
    
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
        String cluster = config.getProperty("cluster.name", "elasticsearch");
        String host = config.getProperty("host", "localhost");
        int port = Integer.parseInt(config.getProperty("port", "9300"));
        
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", cluster).build();
        client = TransportClient.builder().settings(settings).build();
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }

    private void checkIndex(String indexName1) {
        boolean exists = client.admin().indices().prepareExists(indexName1).execute().actionGet().isExists();
        if (!exists) {
            client.admin().indices().prepareCreate(indexName1).execute().actionGet();
            log.log(Level.INFO, "ESWriter: created new index {0}", indexName1);
        }
    }
    
    public void addStatus(Date createdAt, String user, String text) {
        try {
            client.prepareIndex(indexName, typeName)
                        .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                                .field("user", user)
                                .field("createdAt", createdAt)
                                .field("message", text)
                            .endObject()
                        )
                .get();
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
