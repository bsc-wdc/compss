/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.wdc.tracing.extrae;

public enum Environment {

    EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE, //
    EXTRAE_LIB, //
    EXTRAE_CONFIG_FILE, //
    LD_PRELOAD;//


    public static final Environment[] REMOVE_ENVIRONMENT_VARIABLES = new Environment[] { //
        Environment.EXTRAE_CONFIG_FILE //
    };

    // Extrae environment variables
    public static final Environment[] CLEAN_ENVIRONMENT_VARIABLES = new Environment[] { //
        Environment.LD_PRELOAD //
    };

}
