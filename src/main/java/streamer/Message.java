package streamer;

import java.util.Date;

/**
 * @author nilstes
 */
public class Message {

    private String message;
    private Date createdAt;
    private String user;

    public Message() {
    }

    public Message(String message, Date createdAt, String user) {
        this.message = message;
        this.createdAt = createdAt;
        this.user = user;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
