package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.ITConstants;
import integratedtoolkit.types.resources.MethodResourceDescription;


public class DecafImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final String SCRIPT_PATH = File.separator + "Runtime" + File.separator+"scripts"+File.separator+"system"+File.separator+"decaf"+File.separator+"run_decaf.sh";
	private String mpiRunner;
    private String dfScript;
    private String dfExecutor;
    private String dfLib;
    private String workingDir;


    public DecafImplementation() {
        // For externalizable
        super();
    }

    public DecafImplementation(String dfScript, String dfExecutor, String dfLib, String workingDir, String mpiRunner, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

        this.mpiRunner = mpiRunner;
        this.workingDir = workingDir;
        this.dfScript = dfScript;
        this.dfExecutor = dfExecutor;
        this.dfLib = dfLib;
    }

    public String getDfScript() {
        return this.dfScript;
    }
    
    public String getDfExecutor() {
        return this.dfExecutor;
    }
    
    public String getDfLib() {
        return this.dfLib;
    }

    public String getWorkingDir() {
        return this.workingDir;
    }

    public String getMpiRunner() {
        return this.mpiRunner;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.DECAF;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MPI RUNNER=").append(mpiRunner);
        sb.append(", DF_SCRIPT=").append(dfScript);
        sb.append(", DF_EXECUTOR=").append(dfExecutor);
        sb.append(", DF_LIBRARY=").append(dfLib);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Decaf Method with script " + dfScript + ", executor " + dfScript + ", library " + dfLib + " and MPIrunner " + mpiRunner;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        mpiRunner = (String) in.readObject();
        dfScript = (String) in.readObject();
        dfExecutor = (String) in.readObject();
        dfLib = (String) in.readObject();
        workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(mpiRunner);
        out.writeObject(dfScript);
        out.writeObject(dfExecutor);
        out.writeObject(dfLib);
        out.writeObject(workingDir);
    }

}
