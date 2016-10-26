package integratedtoolkit.log;

import integratedtoolkit.types.exceptions.NonInstantiableException;


public final class Loggers {

    // Integrated Toolkit
    public static final String IT = "integratedtoolkit";

    // Loader
    public static final String LOADER = IT + ".Loader";
    public static final String LOADER_UTILS = IT + ".LoaderUtils";

    // API
    public static final String API = IT + ".API";

    // XML Parsers
    public static final String XML_PARSER = IT + ".Xml";
    public static final String XML_RESOURCES = XML_PARSER + ".Resources";
    public static final String XML_PROJECT = XML_PARSER + ".Project";

    // Resources
    public static final String RESOURCES = IT + ".Resources";

    // Components
    public static final String ALL_COMP = IT + ".Components";

    public static final String TP_COMP = ALL_COMP + ".TaskProcessor";
    public static final String TD_COMP = ALL_COMP + ".TaskDispatcher";
    public static final String RM_COMP = ALL_COMP + ".ResourceManager";
    public static final String CM_COMP = ALL_COMP + ".CloudManager";
    public static final String ERROR_MANAGER = ALL_COMP + ".ErrorManager";
    public static final String TRACING = ALL_COMP + ".Tracing";

    public static final String TA_COMP = TP_COMP + ".TaskAnalyser";
    public static final String DIP_COMP = TP_COMP + ".DataInfoProvider";

    public static final String TS_COMP = TD_COMP + ".TaskScheduler";
    public static final String JM_COMP = TD_COMP + ".JobManager";
    public static final String FTM_COMP = TD_COMP + ".FileTransferManager";

    public static final String CONNECTORS = IT + ".Connectors";
    public static final String CONNECTORS_UTILS = IT + ".ConnectorsUtils";

    // Worker
    public static final String WORKER = IT + ".Worker";
    public static final String WORKER_EXEC_MANAGER = WORKER + ".ExecManager";
    public static final String WORKER_POOL = WORKER + ".ThreadPool";
    public static final String WORKER_EXECUTOR = WORKER + ".Executor";
    public static final String WORKER_INVOKER = WORKER_EXECUTOR + ".Invoker";
    public static final String WORKER_DATA_MANAGER = WORKER + ".DataManager";

    // Communications
    public static final String COMM = IT + ".Communication";

    // Connectors
    public static final String CONN = IT + ".Connectors";

    // Storage
    public static final String STORAGE = IT + ".Storage";


    private Loggers() {
        throw new NonInstantiableException("Loggers should not be instantiated");
    }

}
