package common.messages;

public class KVReplyMessage implements KVMessage
{
    private String key;
    private String value;
    private StatusType status;

    public KVReplyMessage(String key, String value, StatusType status)
    {
        this.key = key;
        this.value = value;
        this.status = status;
    }

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
    @Override
    public String getKey() {
        return key;
    }

	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
    @Override
    public String getValue() {
        return value;
    }

	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
    @Override
    public StatusType getStatus() {
        return status;
    }
	
    @Override
    public String getStatusString() {
	switch(this.getStatus()) {
	    case this.StatusType.GET:
		return "GET";
	      
	    case this.StatusType.GET_SUCCESS:
		return "GET_SUCCESS";
	    
	    case this.StatusType.GET_ERROR:
		return "GET_ERROR";
	  
	    case this.StatusType.PUT:
		return "PUT";
	
	    case this.StatusType.PUT_SUCCESS:
		return "PUT_SUCCESS";

	    case this.StatusType.PUT_ERROR:
		return "PUT_ERROR";

	    case this.StatusType.PUT_UPDATE:
		return "PUT_UPDATE";

	    case this.StatusType.DELETE_SUCCESS:
		return "DELETE_SUCCESS";

	    case this.StatusType.DELETE_ERROR:
		return "DELETE_ERROR";

	    default:
		return NULL;
	}
	
    }	
}
