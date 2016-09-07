package integratedtoolkit.connectors.vmm;

import org.json.JSONException;
import org.json.JSONObject;


public class VMDescription extends VMRequest {

    /*
     * { "id": "vmId1", "name": "myVm1", "image": "debianImageId", "cpus": 4, "ramMb": 4096, "diskGb": 4, "state":
     * "active", "ipAddress": "192.168.1.2", "hostName": "computeHost1", "dateCreated": "Tue May 20 13:30:33 CEST 2014",
     * "applicationId": "appId1" }
     */
    private String id;
    private String state;
    private String ipAddress;
    private String hostName;


    // private String dateCreated;
    /**
     * 
     */
    public VMDescription() {
        super();
    }

    /**
     * @param name
     * @param image
     * @param cpus
     * @param ramMb
     * @param diskGb
     * @param applicationId
     */
    public VMDescription(String id, String name, String image, int cpus, int ramMb, int diskGb, String state, String ipAddress,
            String hostName, String dateCreated, String applicationId) {
        super(name, image, cpus, ramMb, diskGb, applicationId);
        this.id = id;
        this.state = state;
        this.ipAddress = ipAddress;
        this.hostName = hostName;
        // this.dateCreated = dateCreated;
    }

    public VMDescription(JSONObject jsonObject) throws JSONException {
        super((String) jsonObject.get("name"), (String) jsonObject.get("image"), (int) jsonObject.get("cpus"),
                (int) jsonObject.get("ramMb"), (int) jsonObject.get("diskGb"), (String) jsonObject.get("applicationId"));
        this.id = (String) jsonObject.get("id");
        this.state = (String) jsonObject.get("state");
        this.ipAddress = (String) jsonObject.get("ipAddress");
        this.hostName = (String) jsonObject.get("hostName");
        // this.dateCreated = (String)jsonObject.get("dateCreated");

    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * @param state
     *            the state to set
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return the ipAddress
     */
    public String getIpAddress() {
        return ipAddress;
    }

    /**
     * @param ipAddress
     *            the ipAddress to set
     */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    /**
     * @return the hostName
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * @param hostName
     *            the hostName to set
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
    /**
     * @return the dateCreated
     *
     *         public String getDateCreated() { return dateCreated; } /**
     * @param dateCreated
     *            the dateCreated to set
     *
     *            public void setDateCreated(String dateCreated) { this.dateCreated = dateCreated; }
     */

}
