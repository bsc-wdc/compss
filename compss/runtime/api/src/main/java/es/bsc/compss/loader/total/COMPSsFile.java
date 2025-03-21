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

import es.bsc.compss.loader.LoaderAPI;
import java.io.File;


public class COMPSsFile extends File {

    private static final long serialVersionUID = 1L;

    private final LoaderAPI api;
    private final Long appId;
    private final String pathname;


    /**
     * Creates a new COMPSsFile instance associated to the given file {@code f} and pointing to the given LoaderAPI
     * {@code api}.
     * 
     * @param api Associated LoaderAPI.
     * @param appId Id of the application accessing the file
     * @param f Associated file.
     */
    public COMPSsFile(LoaderAPI api, Long appId, File f) {
        super(f.getAbsolutePath());
        this.api = api;
        this.appId = appId;
        this.pathname = f.getAbsolutePath();
    }

    @Override
    public boolean createNewFile() throws java.io.IOException {
        System.out.println("COMPSs WARNING: You are creating a new file from a file " + this.pathname
            + " which has been used in a task. This could make your program to not work as expected.");
        return super.createNewFile();
    }

    @Override
    public long getFreeSpace() {
        System.out.println("COMPSs WARNING: You are getting the free space in file " + this.pathname
            + ". This has been used in a task. This could make your program to not work as expected.");
        return super.getFreeSpace();
    }

    @Override
    public long getUsableSpace() {
        System.out.println("COMPSs WARNING: You are getting the usable space in file " + this.pathname
            + ". This has been used in a task. This could make your program to not work as expected.");
        return super.getUsableSpace();
    }

    @Override
    public long getTotalSpace() {
        System.out.println("COMPSs WARNING: You are getting the total space in file " + this.pathname
            + ". This has been used in a task. This could make your program to not work as expected.");
        return super.getTotalSpace();
    }

    @Override
    public boolean exists() {
        System.out.println("COMPSs WARNING: You are checking if file " + this.pathname
            + " exists. This has been used in a task. This could make your program to not work as expected.");
        return super.exists();
    }

    @Override
    public boolean delete() {
        return this.api.deleteFile(appId, this.pathname);
    }

    /**
     * Returns the File object after synchronizing its content.
     * 
     * @return File File object after synchronizing its content.
     */
    public File synchFile() {
        this.api.getFile(appId, this.pathname);
        return new File(this.pathname);
    }

    /**
     * Synchronizes the given COMPSsFile {@code f}.
     * 
     * @param f COMPSsFile.
     * @return File object after synchronizing its content.
     */
    public static File synchFile(COMPSsFile f) {
        return f.synchFile();
    }

}
