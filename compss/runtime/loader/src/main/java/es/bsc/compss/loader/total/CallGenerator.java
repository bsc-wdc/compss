/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.loader.total;

public class CallGenerator {

    // ObjectRegistry methods
    private static final String GET_INTERNAL_OBJECT = ".getInternalObject(";
    private static final String NEW_OBJECT_ACCESS = ".newObjectAccess(";
    private static final String SERIALIZE_LOCALLY = ".serializeLocally(";

    private static final String NEW_COMPSS_FILE = ".newCOMPSsFile(";
    private static final String DELETE_FILE = ".deleteFile(";

    private static final String GET_CANONICAL_PATH = ".getCanonicalPath(";


    /**
     * Constructs the instruction to get the internal object registered for a representative.
     *
     * @param itOR name of the ObjectRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param val name of the variable containing the accessed object
     * @return instruction calling the runtime to return the object internally stored in the OR
     */
    public static String oRegGetInternalObject(String itOR, String itAppId, String val) {
        return itOR + GET_INTERNAL_OBJECT + "(java.lang.Long)" + itAppId + "," + val + ")";
    }

    /**
     * Constructs the instruction to register a new object access.
     *
     * @param itOR name of the ObjectRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param val name of the variable containing the accessed object
     * @return instruction calling the runtime to register a new object access
     */
    public static String oRegNewObjectAccess(String itOR, String itAppId, String val) {
        return itOR + NEW_OBJECT_ACCESS + "(java.lang.Long)" + itAppId + "," + val + ")";
    }

    /**
     * Constructs the instruction to register a new object access.
     *
     * @param itOR name of the ObjectRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param val name of the variable containing the accessed object
     * @param isWriter does the access modify the object
     * @return instruction calling the runtime to register a new object access
     */
    public static String oRegNewObjectAccess(String itOR, String itAppId, String val, boolean isWriter) {
        return itOR + NEW_OBJECT_ACCESS + "(java.lang.Long)" + itAppId + "," + val + "," + " (boolean)" + isWriter
            + ")";
    }

    /**
     * Constructs the instruction to get the serialize the registered object.
     *
     * @param itOR name of the ObjectRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param val name of the variable containing object to register
     * @return instruction calling the runtime to serialize the object stored in the OR
     */
    public static String oRegSerializeLocally(String itOR, String itAppId, String val) {
        return itOR + SERIALIZE_LOCALLY + "(java.lang.Long)" + itAppId + "," + val + ")";
    }

    /**
     * Constructs the instruction to generate a new COMPSsFile object.
     * 
     * @param itSRVar name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param callPars parameters to create
     * @return instruction creating a new COMPSsFile instance for the given File
     */
    public static String newCOMPSsFile(String itSRVar, String itAppId, StringBuilder callPars) {
        return itSRVar + NEW_COMPSS_FILE + "(java.lang.Long)" + itAppId + "," + callPars + ");";
    }

    /**
     * Constructs the instruction to request the runtime the deletion of a file.
     * 
     * @param itApiVar name of the variable containing the runtime
     * @param itAppId name of the variable containing the AppId
     * @return instruction to request to the runtime the deletion of a file
     */
    public static String deleteFile(String itApiVar, String itAppId) {
        return itApiVar + DELETE_FILE + "(java.lang.Long)" + itAppId + "," + "$0" + GET_CANONICAL_PATH + "));";
    }

}
