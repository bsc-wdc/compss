package integratedtoolkit.connectors.vmm;

public class VMRequest {

    private String name;
    private String image;
    private int cpus;
    private int ramMb;
    private int diskGb;
    private String applicationId;


    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the image
     */
    public String getImage() {
        return image;
    }

    /**
     * @param image
     *            the image to set
     */
    public void setImage(String image) {
        this.image = image;
    }

    /**
     * @return the cpus
     */
    public int getCpus() {
        return cpus;
    }

    /**
     * @param cpus
     *            the cpus to set
     */
    public void setCpus(int cpus) {
        this.cpus = cpus;
    }

    /**
     * @return the ramMb
     */
    public int getRamMb() {
        return ramMb;
    }

    /**
     * @param ramMb
     *            the ramMb to set
     */
    public void setRamMb(int ramMb) {
        this.ramMb = ramMb;
    }

    /**
     * @return the diskGb
     */
    public int getDiskGb() {
        return diskGb;
    }

    /**
     * @param diskGb
     *            the diskGb to set
     */
    public void setDiskGb(int diskGb) {
        this.diskGb = diskGb;
    }

    /**
     * @return the applicationId
     */
    public String getApplicationId() {
        return applicationId;
    }

    /**
     * @param applicationId
     *            the applicationId to set
     */
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    /**
     * @param name
     * @param image
     * @param cpus
     * @param ramMb
     * @param diskGb
     * @param applicationId
     */
    public VMRequest(String name, String image, int cpus, int ramMb, int diskGb, String applicationId) {
        super();
        this.name = name;
        this.image = image;
        this.cpus = cpus;
        this.ramMb = ramMb;
        this.diskGb = diskGb;
        this.applicationId = applicationId;
    }

    public VMRequest() {

    }

}
