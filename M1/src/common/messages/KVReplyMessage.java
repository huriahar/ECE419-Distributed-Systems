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

    public KVReplyMessage(String key, String value, String status){
        
        this.key = key;
        this.value = value;
        this.status = getStatusFromString(status);
    }

    /**
     * @return the key that is associated with this message, 
     *      null if not key is associated.
     */
    @Override
    public String getKey() {
        return key;
    }

    /**
     * @return the value that is associated with this message, 
     *      null if not value is associated.
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

    private StatusType getStatusFromString (String status) {
        StatusType statusType;
        switch(status){
            case "GET":
                statusType = StatusType.GET;
                break;
            case "GET_SUCCESS":
                statusType = StatusType.GET_SUCCESS;
                break;
            case "GET_ERROR":
                statusType = StatusType.GET_ERROR;
                break;
            case "PUT":
                statusType = StatusType.PUT;
                break;
            case "PUT_SUCCESS":
                statusType = StatusType.PUT_SUCCESS;
                break;
            case "PUT_UPDATE":
                statusType = StatusType.PUT_UPDATE;
                break;
            case "PUT_ERROR":
                statusType = StatusType.PUT_ERROR;
                break;
            case "DELETE_SUCCESS":
                statusType = StatusType.DELETE_SUCCESS;
                break;
            case "DELETE_ERROR":
                statusType = StatusType.DELETE_ERROR;
                break;
            default:
                statusType = StatusType.PUT_ERROR;
                break;
        }
        return statusType;
    }
    
    public String getStatusString() {
        switch(this.getStatus()) {
            case GET:
            return "GET";
              
            case GET_SUCCESS:
            return "GET_SUCCESS";
            
            case GET_ERROR:
            return "GET_ERROR";
          
            case PUT:
            return "PUT";
        
            case PUT_SUCCESS:
            return "PUT_SUCCESS";

            case PUT_ERROR:
            return "PUT_ERROR";

            case PUT_UPDATE:
            return "PUT_UPDATE";

            case DELETE_SUCCESS:
            return "DELETE_SUCCESS";

            case DELETE_ERROR:
            return "DELETE_ERROR";

            default:
            return null;
        }   
    }   
}
