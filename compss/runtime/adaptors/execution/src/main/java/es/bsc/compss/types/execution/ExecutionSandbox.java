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

package es.bsc.compss.types.execution;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.FileOperations;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecutionSandbox {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);


    private static enum DataComparison {
        SAME_DATA_VERSION, SAME_DATA_MAJOR_VERSION, SAME_DATA_MINOR_VERSION, DIFF_DATA, ERROR
    }


    private final File folder;
    private final String folderAbsolutePath;
    private final boolean isSpecific;


    /**
     * Constructs a new TaskWorkingDir.
     *
     * @param folder task execution directory
     * @param isSpecific is a specific directory
     */
    public ExecutionSandbox(File folder, boolean isSpecific) {
        this.folder = folder;
        this.folderAbsolutePath = folder.getAbsolutePath() + File.separator;
        this.isSpecific = isSpecific;
    }

    /**
     * Returns the execution directory.
     *
     * @return execution directory
     */
    public File getFolder() {
        return this.folder;
    }

    /**
     * Creates the sandbox directory.
     */
    public void create() throws IOException {
        if (!isSpecific) {
            // Clean-up previous versions if any
            if (folder.exists()) {
                LOGGER.debug("Deleting folder " + folder.toString());
                if (!folder.delete()) {
                    LOGGER.warn("Cannot delete working dir folder: " + folder.toString());
                }
            }
        }
        // Create structures
        Files.createDirectories(folder.toPath());
    }

    /**
     * Adds a file to the sandbox.
     *
     * @param dataId Management Id of the data being added to the sandbox
     * @param externalFile File with the data out of the sandbox
     * @param internalName name of the file inside the sandbox
     * @return Path of the file within the sandbox
     * @throws IOException Exception with file operations
     */
    public String addFile(String dataId, File externalFile, String internalName) throws IOException {
        String internalPath = this.folderAbsolutePath + internalName;
        final File internalFile = new File(internalPath);
        if (externalFile.exists()) {
            // IN or INOUT File creating a symbolic link
            Path internalP = internalFile.toPath();
            if (internalFile.exists()) {
                if (Files.isSymbolicLink(internalP)) {
                    internalPath = manageOverlapInSymlink(dataId, internalName, internalP, externalFile);
                } else {
                    // IN / INOUT File is originally located in the working dir
                    LOGGER.info("IN or INOUT file is originally located in the working dir: " + internalFile.getName());
                    return null;
                }
            } else {
                Path externalP = externalFile.toPath();
                LOGGER.debug("Creating symlink " + internalP + " pointing to " + externalP);
                Files.createSymbolicLink(internalP, externalP);
            }
        } else {
            // OUT file
            if (internalFile.exists()) {
                // Overlap with previous in sandBox. Update in sandBox name.
                internalPath = this.folderAbsolutePath + dataId + "_" + internalName;
            }
        }
        return internalPath;
    }

    private String manageOverlapInSymlink(String dataId, String internalName, Path existingLink, File external)
        throws IOException {
        String resultLink = null;

        final Path oExternalP = Files.readSymbolicLink(existingLink);
        final String oRename = oExternalP.getFileName().toString();

        final String nRename = external.getName();

        LOGGER.debug("Checking if " + nRename + " is equal to " + oRename);
        switch (checkDataVersion(nRename, oRename)) {
            case SAME_DATA_VERSION:
            case SAME_DATA_MINOR_VERSION:
                resultLink = existingLink.toString();
                break;
            case SAME_DATA_MAJOR_VERSION: {
                Path nExternalP = external.toPath();
                LOGGER.debug("Updating Symbolic link " + existingLink + "->" + nExternalP.toString());
                Files.delete(existingLink);
                Files.createSymbolicLink(existingLink, nExternalP);
                resultLink = existingLink.toString();
            }
                break;
            case DIFF_DATA: {
                Path nExternalP = external.toPath();
                String newInternalName = this.folderAbsolutePath + dataId + "_" + internalName;
                File newSandboxFile = new File(newInternalName);
                Path nInternalP = newSandboxFile.toPath();
                LOGGER.debug("Creating Symbolic link " + nInternalP.toString() + "->" + nExternalP.toString());
                Files.createSymbolicLink(newSandboxFile.toPath(), nExternalP);
                resultLink = newInternalName;
            }
                break;
            case ERROR:
                LOGGER.warn("ERROR: There was an error extracting data versions for " + oRename + " and " + nRename);
        }
        return resultLink;
    }

    /**
     * Check whether file1 corresponds to a file with a higher version than file2.
     *
     * @param file1 first file name
     * @param file2 second file name
     * @return True if file1 has a higher version. False otherwise (This includes the case where the name file's format
     *         is not correct)
     */
    private DataComparison checkDataVersion(String file1, String file2) {
        String[] version1array = file1.split("_")[0].split("v");
        String[] version2array = file2.split("_")[0].split("v");
        if (version1array.length < 2 || version2array.length < 2) {
            return DataComparison.ERROR;
        }
        if (version1array[0].compareTo(version2array[0]) == 0) {
            // same data
            int version1int;
            int version2int;
            try {
                version1int = Integer.parseInt(version1array[1]);
                version2int = Integer.parseInt(version2array[1]);
                if (version1int > version2int) {
                    return DataComparison.SAME_DATA_MAJOR_VERSION;
                } else if (version1int == version2int) {
                    return DataComparison.SAME_DATA_VERSION;
                } else {
                    return DataComparison.SAME_DATA_MINOR_VERSION;
                }
            } catch (NumberFormatException e) {
                return DataComparison.ERROR;
            }
        } else {
            return DataComparison.DIFF_DATA;
        }
    }

    /**
     * Removes a file from the sandbox and places it out of the sandbox.
     *
     * @param internalPath Path where the file can be find inside the sandbox
     * @param externalPath Path where the file should be outside the sandbox
     * @throws IOException Exception with file operations
     */
    public void removeFile(String internalPath, String externalPath) throws IOException {
        File internalFile = new File(internalPath);
        File externalFile = new File(externalPath);

        Path iPath = internalFile.toPath();
        if (externalFile.exists()) {
            // IN, INOUT
            if (Files.isSymbolicLink(iPath)) {
                // If a symbolic link is created remove it
                LOGGER.debug("Deleting symlink " + iPath);
                FileOperations.deleteFile(internalFile, LOGGER);

            } else {
                // Rewrite inout param by moving the new file to the renaming
                LOGGER.debug("Moving from " + internalFile + " to " + externalFile);
                FileOperations.deleteFile(externalFile, LOGGER);
                FileOperations.moveFile(internalFile, externalFile, LOGGER);
            }

        } else { // OUT
            if (Files.isSymbolicLink(iPath)) {
                // Unexpected case
                String msg = "ERROR: Unexpected case. A Problem occurred with File " + internalFile
                    + ". Either this file or the original name " + externalPath + " does not exist.";
                LOGGER.error(msg);
            } else {
                // If an output file is created move to the renamed path (OUT Case)
                FileOperations.moveFile(internalFile, externalFile, LOGGER);
            }
        }
    }

    /**
     * Removes the sandbox directory.
     */
    public void clean() {
        if (!isSpecific) {
            // Only clean task sandbox if it is not specific
            if (folder != null && folder.exists() && folder.isDirectory()) {
                try {
                    LOGGER.debug("Deleting sandbox " + folder.toPath());
                    FileOperations.deleteFile(folder, LOGGER);
                } catch (IOException e) {
                    LOGGER.warn("Error deleting sandbox " + e.getMessage(), e);
                }
            }
        }
    }

}
