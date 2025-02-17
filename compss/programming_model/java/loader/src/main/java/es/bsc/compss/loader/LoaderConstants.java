/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.loader;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSs;
import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.loader.total.ArrayAccessWatcher;
import es.bsc.compss.loader.total.COMPSsGroupLoader;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.types.annotations.Orchestration;
import java.io.File;
import java.util.Arrays;
import java.util.List;


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
    public static final String CLASS_LOADERAPI = LoaderAPI.class.getCanonicalName();
    public static final String CLASS_ANNOTATIONS_ORCHESTRATION = Orchestration.class.getCanonicalName();
    public static final String CLASS_COMPSS_CONSTANTS = COMPSsConstants.class.getCanonicalName();
    public static final String CLASS_COMPSS_GROUP_LOADER = COMPSsGroupLoader.class.getCanonicalName();
    public static final String CLASS_COMPSS_GROUP = COMPSsGroup.class.getCanonicalName();

    // Strings for Loader variables
    public static final String STR_COMPSS_PREFIX = "compss";
    public static final String STR_COMPSS_API = "Api";
    public static final String STR_COMPSS_STREAM_REGISTRY = "SR";
    public static final String STR_COMPSS_OBJECT_REGISTRY = "OR";
    public static final String STR_COMPSS_APP_ID = "AppId";

    // Supported Stream Types
    private static final List<String> SUPPORTED_STREAM_TYPES = Arrays.asList("FileInputStream", // 58,700
        "FileOutputStream", // 57,700
        "InputStreamReader", // 61,200
        "BufferedReader", // 36,400
        "FileWriter", // 33,900
        "PrintWriter", // 35,200
        "FileReader", // 16,800
        "OutputStreamWriter", // 15,700
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


    /**
     * Get the supported File streams.
     * 
     * @return list of supported file streams.
     */
    public static List<String> getSupportedStreamTypes() {
        return SUPPORTED_STREAM_TYPES;
    }

}
