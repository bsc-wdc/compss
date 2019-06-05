package es.bsc.compss.invokers.external.piped;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.executor.external.piped.ControlPipePair;
import es.bsc.compss.executor.external.piped.PipedMirror;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.types.execution.InvocationContext;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


public class CMirror extends PipedMirror {

    // C worker relative path
    private static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator
            + "bindings-common" + File.separator + "lib";
    private static final String C_PIPER = "c_piper.sh";
    private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";
    private static final String C_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "c"
            + File.separator + "lib";


    /**
     * Creates a new CMirror instance with the given context and size.
     * 
     * @param context Context.
     * @param size Thread size.
     */
    public CMirror(InvocationContext context, int size) {
        super(context, size);
        init(context);
    }

    @Override
    public String getPipeBuilderContext() {
        StringBuilder cmd = new StringBuilder();
        return cmd.toString();
    }

    @Override
    public String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe) {
        // Specific launch command is of the form: binding bindingExecutor bindingArgs
        StringBuilder cmd = new StringBuilder();

        String installDir = context.getInstallDir();

        cmd.append(installDir).append(PIPER_SCRIPT_RELATIVE_PATH).append(C_PIPER).append(TOKEN_SEP);

        String computePipes = basePipePath + "compute";
        cmd.append(size).append(TOKEN_SEP);
        for (int i = 0; i < size; ++i) {
            cmd.append(computePipes).append(i).append(".outbound").append(TOKEN_SEP);
        }

        cmd.append(size).append(TOKEN_SEP);
        for (int i = 0; i < size; ++i) {
            cmd.append(computePipes).append(i).append(".inbound").append(TOKEN_SEP);
        }
        cmd.append(pipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(pipe.getInboundPipe());
        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(InvocationContext context) {
        String ldLibraryPath = System.getenv(LIBRARY_PATH_ENV);
        CParams cParams = (CParams) context.getLanguageParams(Lang.C);
        if (ldLibraryPath == null) {
            ldLibraryPath = cParams.getLibraryPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + cParams.getLibraryPath());
        }

        // Add C and commons libs
        ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + C_LIB_RELATIVE_PATH);
        ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + BINDINGS_RELATIVE_PATH);
        Map<String, String> env = new HashMap<>();
        env.put(LIBRARY_PATH_ENV, ldLibraryPath);
        return env;
    }
}