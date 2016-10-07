package streamer;

import java.net.InetAddress;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
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
        this.indexName = indexName;
        this.typeName = typeName;   
        
        Settings settings = Settings.settingsBuilder()
            .put("cluster.name", "elasticsearch").build();
        client = TransportClient.builder().settings(settings).build();
	client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            
        boolean exists = client.admin().indices()
            .prepareExists(indexName)
            .execute().actionGet().isExists();
        if(!exists) {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            log.log(Level.INFO, "ESWriter: created new index {0}", indexName);
        }
        log.info("ESWriter: ok");
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
}