package integratedtoolkit.api;


public interface IntegratedToolkit {

    // File access modes
    public enum OpenMode {

        READ,
        WRITE,
        APPEND;
    }

// Interface Operations
    public String getApplicationDirectory();

    public void startIT();

    public void stopIT(boolean terminate);

    public String openFile(String fileName, OpenMode m);

    public boolean deleteFile(String fileName);

}
