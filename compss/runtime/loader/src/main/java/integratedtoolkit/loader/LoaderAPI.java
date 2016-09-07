package integratedtoolkit.loader;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.loader.total.ObjectRegistry;


public interface LoaderAPI {

    /**
     * Returns the renaming of the file version opened
     * 
     * @param fileName
     * @param mode
     * @return
     */
    public String openFile(String fileName, DataDirection mode);

    /**
     * Returns the renaming of the last file version just transferred
     * 
     * @param fileName
     * @param destDir
     * @return
     */
    String getFile(String fileName, String destDir);

    /**
     * Returns a copy of the last object version
     * 
     * @param o
     * @param hashCode
     * @param destDir
     * @return
     */
    Object getObject(Object o, int hashCode, String destDir);

    /**
     * Serializes the given object
     * 
     * @param o
     * @param hashCode
     * @param destDir
     */
    void serializeObject(Object o, int hashCode, String destDir);

    /**
     * Sets the object Registry instance
     *
     * @param oReg
     */
    void setObjectRegistry(ObjectRegistry oReg);

    /**
     * Returns the directory where to store temporary files
     * 
     * @return
     */
    String getTempDir();

}
