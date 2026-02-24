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

package es.bsc.compss.loader.total;

import es.bsc.compss.loader.LoaderConstants;


public class CallGenerator {

    // JavaWorkflow methods
    private static final String OBJECT_ACCESS = ".getObject(";
    private static final String OBJECT_PARAM = ".accessAsTaskParam(";
    private static final String OBJECT_VALUE = ".getRegisteredObjectValue(";
    private static final String OBJECT_DELETE = ".removeObject(";

    // File access methods
    private static final String NEW_FILTER_STREAM = ".newFilterStream(";
    private static final String NEW_COMPSS_FILE = ".newCOMPSsFile(";
    private static final String ADD_TASK_FILE = ".addTaskFile(";
    private static final String DELETE_TASK_FILE = ".deleteTaskFile(";
    private static final String IS_TASK_FILE = ".isTaskFile(";
    private static final String OPEN_FILE = ".openFile(";

    private static final String COMPSS_FILE_SYNCH = "synchFile()";
    private static final String STREAM_CLOSED = ".streamClosed(";
    private static final String DELETE_FILE = ".deleteFile(";

    private static final String GET_CANONICAL_PATH = ".getCanonicalPath(";


    /**
     * Constructs the instruction to get the internal object registered for a representative.
     *
     * @param itWf name of the JavaWorkflow variable
     * @param val name of the variable containing the accessed object
     * @return instruction calling the runtime to return the object internally stored in the OR
     */
    public static String getRegisteredObjectValue(String itWf, String val) {
        return itWf + OBJECT_VALUE + val + ")";
    }

    /**
     * Constructs the instruction to register a new write access to an object.
     *
     * @param itWf name of the JavaWorkflow variable
     * @param val name of the variable containing the accessed object
     * @return instruction calling the runtime to register a new object access
     */
    public static String wfAccessObject(String itWf, String val) {
        return itWf + OBJECT_ACCESS + val + ")";
    }

    /**
     * Constructs the instruction to register a new object access.
     *
     * @param itWf name of the JavaWorkflow variable
     * @param val name of the variable containing the accessed object
     * @param isWriter does the access modify the object
     * @return instruction calling the runtime to register a new object access
     */
    public static String wfAccessObject(String itWf, String val, boolean isWriter) {
        return itWf + OBJECT_ACCESS + val + "," + " (boolean)" + isWriter + ")";
    }

    /**
     * Constructs the instruction to register a new object access.
     *
     * @param itWf name of the JavaWorkflow variable
     * @param val name of the variable containing the accessed object
     * @return instruction calling the runtime to register a new object access
     */
    public static String wfObjectParameter(String itWf, String val) {
        return itWf + OBJECT_PARAM + val + ")";
    }

    /**
     * Constructs the instruction to delete an object value.
     *
     * @param itWf name of the JavaWorkflow variable
     * @param val name of the variable containing the accessed object
     * @return instruction calling the OR to remove the object
     */
    public static String wfDeleteObject(String itWf, String val) {
        return itWf + OBJECT_DELETE + val + ")";
    }

    /**
     * Constructs the instruction to generate a new COMPSsFile object.
     *
     * @param itWf name of the variable containing the workflow
     * @param callPars parameters to create
     * @return instruction creating a new COMPSsFile instance for the given File
     */
    public static String newCOMPSsFile(String itWf, StringBuilder callPars) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + NEW_COMPSS_FILE + itWf + "," + callPars + ")";
    }

    /**
     * Constructs the instruction to request the runtime the deletion of a file.
     *
     * @param itWf name of the variable containing the workflow
     * @param file file to delete
     * @return instruction to request to the runtime the deletion of a file
     */
    public static String deleteFile(String itWf, String file) {
        return itWf + DELETE_FILE + file + GET_CANONICAL_PATH + "), true, true)";
    }

    /**
     * Constructs the instruction to synchronize a file given as a parameter.
     * 
     * @param file file being synchronized
     * @return instruction to synchronize a file given as a parameter
     */
    public static String synchFile(String file) {
        return file + COMPSS_FILE_SYNCH;
    }

    /**
     * Constructs the instruction to create a new Stream.
     *
     * @param itWf name of the variable containing the workflow
     * @param streamClass stream type
     * @param callPars parameters to call the stream constructor
     * @return instruction to create a new stream
     */
    public static String newStreamClass(String itWf, String streamClass, StringBuilder callPars) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + ".new" + streamClass + "(" + itWf + "," + callPars + ")";
    }

    /**
     * Constructs the instruction to create a new FilterStream.
     *
     * @param itWf name of the variable containing the workflow
     * @param par name of the paremeter to pass in to the newFilterStream constructor
     * @return instruction to create a newFilterStream
     */
    public static String newFilterStream(String itWf, String par) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + NEW_FILTER_STREAM + itWf + "," + par + ", (Object)$_); }";
    }

    /**
     * Constructs an instruction to close a stream.
     *
     * @param itWf name of the variable containing the workflow
     * @return instruction to register the stream closing
     */
    public static String closeStream(String itWf) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + STREAM_CLOSED + itWf + ", $0)";
    }

    /**
     * Constructs an instruction to register a file as a task parameter.
     *
     * @param file file
     * @return instruction to register a file as a task parameter
     */
    public static String addTaskFile(String file) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + ADD_TASK_FILE + file + ")";
    }

    /**
     * Constructs an instruction to check whether a file was passed as a task parameter or not.
     *
     * @param parId file to be checked
     * @return instruction to check whether a file was passed as a task parameter or not
     */
    public static String isTaskFile(String parId) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + IS_TASK_FILE + parId + ")";
    }

    /**
     * Constructs an instruction to call the deleteTaskFile method of the StreamRegistry.
     *
     * @param file file to be checked
     * @return instruction removing the file from the SR
     */
    public static String removeTaskFile(String file) {
        return LoaderConstants.CLASS_STREAM_REGISTRY + DELETE_TASK_FILE + file + ")";
    }

    /**
     * Constructs an instruction to call the openFile method of the Runtime API.
     *
     * @param itWf name of the variable containing the workflow
     * @param file variable containing the file
     * @param direction operation performed on the file (IN, OUT, INOUT)
     * @return
     */
    public static String openFile(String itWf, String file, String direction) {
        return itWf + OPEN_FILE + file + ", " + direction + ")";
    }

}
