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

import es.bsc.compss.loader.LoaderAPI;
import java.io.File;


public class COMPSsFile extends File {

    /**
     * Class which intercepts.
     */
    private static final long serialVersionUID = 1L;

    private LoaderAPI api;
    private String pathname;

    /**
     * TODO javadoc.
     *
     * @param api description
     * @param f   description
     */
    public COMPSsFile(LoaderAPI api, File f) {
        super(f.getAbsolutePath());
        this.api = api;
        this.pathname = f.getAbsolutePath();
    }

    @Override
    public boolean createNewFile() throws java.io.IOException {
        System.out.println("COMPSs WARNING: You are creating a new file from a file " + pathname
                + " which has been used in a task. This could make your program to not work as expected.");
        return super.createNewFile();
    }

    @Override
    public long getFreeSpace() {
        System.out.println("COMPSs WARNING: You are getting the free space in file " + pathname
                + ". This has been used in a task. This could make your program to not work as expected.");
        return super.getFreeSpace();
    }

    @Override
    public long getUsableSpace() {
        System.out.println("COMPSs WARNING: You are getting the usable space in file " + pathname
                + ". This has been used in a task. This could make your program to not work as expected.");
        return super.getUsableSpace();
    }

    @Override
    public long getTotalSpace() {
        System.out.println("COMPSs WARNING: You are getting the total space in file " + pathname
                + ". This has been used in a task. This could make your program to not work as expected.");
        return super.getTotalSpace();
    }

    @Override
    public boolean exists() {
        System.out.println("COMPSs WARNING: You are checking if file " + pathname
                + " exists. This has been used in a task. This could make your program to not work as expected.");
        return super.exists();
    }

    @Override
    public boolean delete() {
        return api.deleteFile(pathname);
    }

    public File synchFile(Long appId) {
        api.getFile(appId, pathname);
        return new File(pathname);
    }

    public static File synchFile(Long appId, COMPSsFile f) {
        return f.synchFile(appId);
    }

}
