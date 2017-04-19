package guidance.utils;

/**
 * @brief GenericClass. This clase includes 5 attributes
 * @author Friman Sanchez
 * @date 2014-10-15
 */
public class GenericFile {

    private String dir = null;
    private String name = null;
    private String finalStatus = null;
    private String fullName = null;
    private String generatedBy = null;


    /**
     * A first constructor for GenericFile class
     * 
     * @param myDir
     * @param myName
     * @param myFinalStatus
     */
    public GenericFile(String myDir, String myName, String myFinalStatus) {
        dir = myDir;
        name = myName;
        finalStatus = myFinalStatus;
        fullName = myDir + "/" + myName;
        generatedBy = null;
    }

    /**
     * A second constructor for GenericFile class
     * 
     * @param myDir
     * @param myName
     * @param myFinalStatus
     * @param myGeneratedBy
     */
    public GenericFile(String myDir, String myName, String myFinalStatus, String myGeneratedBy) {
        dir = myDir;
        name = myName;
        finalStatus = myFinalStatus;
        fullName = myDir + "/" + myName;
        generatedBy = myGeneratedBy;

        // System.out.println("------------------------------------");
        // System.out.println("[GenericFile] : " + fullName);
        // System.out.println("[GenericFile] : " + finalStatus);
    }

    /**
     * A method to set dir field
     * 
     * @param myDir
     */
    public void setDir(String myDir) {
        dir = myDir;
    }

    /**
     * A method to set name field
     * 
     * @param myName
     */
    public void setName(String myName) {
        name = myName;
    }

    /**
     * A method to set finalStatus field
     * 
     * @param myFinalStatus
     */
    public void setFinalStatus(String myFinalStatus) {
        finalStatus = myFinalStatus;
    }

    /**
     * A method to set fullName field
     * 
     * @param myFullName
     */
    public void setFullName(String myFullName) {
        fullName = myFullName;
    }

    /**
     * A method to get dir field
     * 
     * @return
     */
    public String getDir() {
        return dir;
    }

    /**
     * A method to get Name field
     * 
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * A method to get finalStatus field
     * 
     * @return
     */
    public String getFinalStatus() {
        return finalStatus;
    }

    /**
     * A method to get fullName field
     * 
     * @return
     */
    public String getFullName() {
        return fullName;
    }

    /**
     * A method to get generatedBy field
     * 
     * @return
     */
    public String getGeneratedBy() {
        return generatedBy;
    }

}
