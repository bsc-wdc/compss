package integratedtoolkit.loader;

import integratedtoolkit.api.IntegratedToolkit.OpenMode;
import integratedtoolkit.loader.total.ObjectRegistry;


public interface LoaderAPI {

	// Returns the renaming of the last file version just transferred
	String getFile(String fileName, String destDir);
	
	// Returns a copy of the last object version
	Object getObject(Object o, int hashCode, String destDir);
	
	// Returns the renaming of the file version opened
	String openFile(String fileName, OpenMode m);
	
	void serializeObject(Object o, int hashCode, String destDir);
	
	void setObjectRegistry(ObjectRegistry oReg);
	
	// Returns the directory where to store temp files
	String getTempDir();
	
}
