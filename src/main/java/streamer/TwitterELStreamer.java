package streamer;

import java.util.Arrays;
import twitter4j.Status;

/**
 * @author nilstes
 */
public class TwitterELStreamer {
  
    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Arguments:\n - 1: ES index name\n - 2: ES index type\n -  3-n: twitter topics to track");
        } else {
            try {
                ESWriter writer = new ESWriter(args[0], args[1]);
                new TweetListener(Arrays.copyOfRange(args, 2, args.length)) {
                    @Override
                    public void onStatus(Status status) {
                        Message message = new Message(status.getText(), status.getCreatedAt(), "@" + status.getUser().getScreenName());
                        writer.addStatus(message);
                    }           
                };
            } catch(Exception e) {
                System.out.println("Failed to start streaming. Error=" + e.getMessage());
            }
        }
    }
}
