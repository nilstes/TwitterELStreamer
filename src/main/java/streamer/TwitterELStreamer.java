package streamer;

import twitter4j.Status;

/**
 * @author nilstes
 */
public class TwitterELStreamer {
  
    public static void main(String[] args) {
        if(args.length != 3) {
            System.out.println("Arguments:\n - 1: twitter topic to track\n - 2: ES index name\n - 3: ES index type");
        } else {
            try {
                ESWriter writer = new ESWriter(args[1], args[2]);
                new TweetListener(args[0]) {
                    @Override
                    public void onStatus(Status status) {
                        writer.addStatus(new Message(status.getText(), status.getCreatedAt(), "@" + status.getUser().getScreenName()));
                    }           
                };
            } catch(Exception e) {
                System.out.println("Failed to start streaming. Error=" + e.getMessage());
            }
        }
    }
}
