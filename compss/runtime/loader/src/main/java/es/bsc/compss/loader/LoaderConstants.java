package es.bsc.compss.loader;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSs;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.loader.total.ArrayAccessWatcher;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.types.annotations.Orchestration;


public class LoaderConstants {

    // Interface
    public static final String ITF_SUFFIX = "Itf";

    // Engine jar
    public static final String ENGINE_JAR = "compss-engine.jar";
    public static final String ENGINE_JAR_WITH_REL_PATH = File.separator + "Runtime" + File.separator + ENGINE_JAR;

    // Packages
    public static final String PACKAGE_COMPSS_ROOT = "es.bsc.compss";
    public static final String PACKAGE_COMPSS_API = PACKAGE_COMPSS_ROOT + ".api";
    public static final String PACKAGE_COMPSS_API_IMPL = PACKAGE_COMPSS_API + ".impl";
    public static final String PACKAGE_COMPSS_LOADER = PACKAGE_COMPSS_ROOT + ".loader";
    public static final String PACKAGE_COMPSS_LOADER_TOTAL = PACKAGE_COMPSS_LOADER + ".total";

    // Classes
    public static final String CLASS_COMPSS_API = COMPSs.class.getCanonicalName();
    public static final String CLASS_COMPSSRUNTIME_API = COMPSsRuntime.class.getCanonicalName();
    public static final String CLASS_STREAM_REGISTRY = StreamRegistry.class.getCanonicalName();
    public static final String CLASS_OBJECT_REGISTRY = ObjectRegistry.class.getCanonicalName();
    public static final String CLASS_APP_ID = Long.class.getCanonicalName();
    public static final String CLASS_ARRAY_ACCESS_WATCHER = ArrayAccessWatcher.class.getCanonicalName();
    public static final String CLASS_COMPSS_API_IMPL = "COMPSsRuntimeImpl";
    public static final String CLASS_LOADERAPI = "LoaderAPI";
    public static final String CLASS_ANNOTATIONS_ORCHESTRATION = Orchestration.class.getCanonicalName();
    public static final String CLASS_COMPSS_CONSTANTS = COMPSsConstants.class.getCanonicalName();

    // Strings for Loader variables
    public static final String STR_COMPSS_PREFIX = "compss";
    public static final String STR_COMPSS_API = "Api";
    public static final String STR_COMPSS_STREAM_REGISTRY = "SR";
    public static final String STR_COMPSS_OBJECT_REGISTRY = "OR";
    public static final String STR_COMPSS_APP_ID = "AppId";

    // Supported Stream Types
    public static final List<String> SUPPORTED_STREAM_TYPES = Arrays.asList("FileInputStream", // 58,700
            "FileOutputStream", // 57,700
            "InputStreamReader", // 61,200
            "BufferedReader", // 36,400
            "FileWriter", // 33,900
            "PrintWriter", // 35,200
            "FileReader", // 16,800
            "OutputStreamWriter", // 15,700
            "BufferedInputStream", // 15,100
            "BufferedOutputStream", // 10,500
            "BufferedWriter", // 11,800
            "PrintStream", // 6,000
            "RandomAccessFile", // 5,000
            "DataInputStream", // 7,000
            "DataOutputStream" // 7,000
    );

    // Custom loader names
    public static final String CUSTOM_LOADER_PREFIX = "es.bsc.compss.loader.";
    public static final String CUSTOM_LOADER_SUFFIX = ".ITAppModifier";

    // Total loader names
    public static final String LOADER_INTERNAL_PREFIX = "es.bsc.compss.";
    public static final String LOADER_IO_PREFIX = "java.io.";

}
