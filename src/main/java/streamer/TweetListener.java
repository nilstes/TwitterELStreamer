package streamer;

import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;


/**
 * @author nilstes
 */
public abstract class TweetListener {

    private static final Logger log = Logger.getLogger(TweetListener.class.getName());

    public TweetListener(String topic) {
        log.log(Level.INFO, "TweetListener: starting to listen to topic {0}", topic);
        
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                TweetListener.this.onStatus(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }
            
            @Override
            public void onException(Exception ex) {
            }

            @Override
            public void onStallWarning(StallWarning sw) {
            }
        };
        twitterStream.addListener(listener);

        FilterQuery tweetFilterQuery = new FilterQuery(); // See 
        tweetFilterQuery.track(new String[]{topic});
        tweetFilterQuery.language(new String[] {"en"});
        twitterStream.filter(tweetFilterQuery);
        
        log.info("TweetListener ok");
    }
    
    public abstract void onStatus(Status status);
}
