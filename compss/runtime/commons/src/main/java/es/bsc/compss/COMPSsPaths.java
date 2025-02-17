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
package es.bsc.compss;

import es.bsc.compss.types.exceptions.NonInstantiableException;
import java.io.File;


/**
 * COMPSS Runtime Paths.
 */
public class COMPSsPaths {

    private static final String COMPSs_HOME = System.getenv(COMPSsConstants.COMPSS_HOME) + File.separator;

    private static final String REL_BINDINGS_DIR = "Bindings" + File.separator;

    private static final String REL_RUNTIME_DIR = "Runtime" + File.separator;

    private static final String REL_SCRIPTS_DIR = REL_RUNTIME_DIR + "scripts" + File.separator;

    private static final String REL_CONFIG_DIR = REL_RUNTIME_DIR + "configuration" + File.separator;
    private static final String REL_CONFIG_XML_DIR = REL_CONFIG_DIR + "xml" + File.separator;
    private static final String REL_CONFIG_XML_RES_DIR = REL_CONFIG_XML_DIR + "resources" + File.separator;
    public static final String REL_RES_SCHEMA = REL_CONFIG_XML_RES_DIR + "resource_schema.xsd";
    private static final String REL_CONFIG_XML_PROJ_DIR = REL_CONFIG_XML_DIR + "projects" + File.separator;
    public static final String REL_PROJECT_SCHEMA = REL_CONFIG_XML_PROJ_DIR + "project_schema.xsd";
    public static final String REL_MPI_CFGS_DIR = REL_CONFIG_DIR + "mpi" + File.separator;

    public static final String LOCAL_RES_SCHEMA = COMPSs_HOME + REL_RES_SCHEMA;
    public static final String LOCAL_PROJECT_SCHEMA = COMPSs_HOME + REL_PROJECT_SCHEMA;

    private static final String GAT_LOC = System.getenv(COMPSsConstants.GAT_LOC);
    public static final String GAT_ADAPTOR_LOCATION = GAT_LOC + File.separator + "lib" + File.separator + "adaptors";

    public static final String REL_DEPS_DIR = "Dependencies" + File.separator;
    public static final String REL_DEPS_EXTRAE_DIR = REL_DEPS_DIR + "extrae" + File.separator;


    /**
     * Private constructor to avoid instantiation.
     */
    private COMPSsPaths() {
        throw new NonInstantiableException("COMPSsPaths");
    }

}
