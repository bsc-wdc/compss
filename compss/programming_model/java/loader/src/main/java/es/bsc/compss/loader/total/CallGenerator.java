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

package es.bsc.compss.loader.total;

public class CallGenerator {

    // ObjectRegistry methods
    private static final String GET_INTERNAL_OBJECT = ".getInternalObject(";
    private static final String NEW_OBJECT_ACCESS = ".newObjectAccess(";
    private static final String SERIALIZE_LOCALLY = ".serializeLocally(";

    // File access methods
    private static final String NEW_FILTER_STREAM = ".newFilterStream(";
    private static final String NEW_COMPSS_FILE = ".newCOMPSsFile(";
    private static final String ADD_TASK_FILE = ".addTaskFile(";
    private static final String IS_TASK_FILE = ".isTaskFile(";
    private static final String OPEN_FILE = ".openFile(";

    private static final String COMPSS_FILE_SYNCH = "COMPSsFile.synchFile(";
    private static final String STREAM_CLOSED = ".streamClosed(";
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
     * @param itSR name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param callPars parameters to create
     * @return instruction creating a new COMPSsFile instance for the given File
     */
    public static String newCOMPSsFile(String itSR, String itAppId, StringBuilder callPars) {
        return itSR + NEW_COMPSS_FILE + "(java.lang.Long)" + itAppId + "," + callPars + ");";
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

    /**
     * Constructs the instruction to synchronize a file given as a parameter.
     * 
     * @param parId file being synchronized
     * @return instruction to synchronize a file given as a parameter
     */
    public static String synchFile(String parId) {
        return COMPSS_FILE_SYNCH + parId + ')';
    }

    /**
     * Constructs the instruction to create a new Stream.
     * 
     * @param itSR name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param streamClass stream type
     * @param callPars parameters to call the stream constructor
     * @return instruction to create a new stream
     */
    public static String newStreamClass(String itSR, String itAppId, String streamClass, StringBuilder callPars) {
        return itSR + ".new" + streamClass + "(" + "(java.lang.Long)" + itAppId + "," + callPars + ");";
    }

    /**
     * Constructs the instruction to create a new FilterStream.
     * 
     * @param itSR name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param par name of the paremeter to pass in to the newFilterStream constructor
     * @return instruction to create a newFilterStream
     */
    public static String newFilterStream(String itSR, String itAppId, String par) {
        return itSR + NEW_FILTER_STREAM + "(java.lang.Long)" + itAppId + "," + par + ", (Object)$_); }";
    }

    /**
     * Constructs an instruction to register a file as a task parameter.
     *
     * @param itSR name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param paramIndex parameterIndex of the file
     * @return instruction to register a file as a task parameter
     */
    public static Object addTaskFile(String itSR, String itAppId, int paramIndex) {
        return itSR + ADD_TASK_FILE + "(java.lang.Long)" + itAppId + "," + "$" + (paramIndex + 1) + ");";
    }

    /**
     * Constructs an instruction to close a stream.
     * 
     * @param itSR name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @return instruction to register the stream closing
     */
    public static String closeStream(String itSR, String itAppId) {
        return itSR + STREAM_CLOSED + "(java.lang.Long)" + itAppId + ", $0);";
    }

    /**
     * Constructs an instruction to check whether a file was passed as a task parameter or not.
     * 
     * @param itSR name of the StreamRegistry variable
     * @param itAppId name of the variable containing the AppId
     * @param parId file to be checked
     * @return instruction to check whether a file was passed as a task parameter or not
     */
    public static String isTaskFile(String itSR, String itAppId, String parId) {
        return itSR + IS_TASK_FILE + "(java.lang.Long)" + itAppId + "," + parId + ")";
    }

    /**
     * Constructs an instruction to call the openFile method of the Runtime API.
     * 
     * @param itApi name of the runtime API
     * @param itAppId name of the variable containing the AppId
     * @param file variable containing the file
     * @param direction operation performed on the file (IN, OUT, INOUT)
     * @return
     */
    public static String openFile(String itApi, String itAppId, String file, String direction) {
        return itApi + OPEN_FILE + "(java.lang.Long)" + itAppId + "," + file + ", " + direction + ")";
    }
}
