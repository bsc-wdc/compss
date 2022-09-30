/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
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
 */
package testContainer;

import container.CONTAINER;

import es.bsc.compss.api.COMPSs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;


public class Main {

    public static void main(String[] args) throws Exception {
        // Empty execution
        CONTAINER.pwdEmpty();

        COMPSs.barrier();

        // ExitValue execution
        Integer ev = CONTAINER.pwdExitValue();
        System.out.println("EXIT VALUE: " + ev);

        // WorkingDir execution
        CONTAINER.pwdWorkingDir();

        COMPSs.barrier();

        // Empty execution
        int n = 3;
        final String msg = "Hello World!";
        final String fileName = "my_file.in";
        writeFileContent(fileName, 1);

        CONTAINER.customParams(n, msg, fileName);

        // WARN: Check job parameters in result script
        COMPSs.barrier();

        // StdOut and StdErr
        final String stdout = "pwd_working_dir.stdout";
        final String stderr = "pwd_working_dir.stderr";
        CONTAINER.pwdWorkingDirStd(stdout, stderr);

        System.out.println("STDOUT:");
        printFileContent(stdout);
        System.out.println("STDERR:");
        printFileContent(stderr);

        CONTAINER.options();

    }

    private static void writeFileContent(String filename, int value) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            fos.write(value);
        } catch (IOException ioe) {
            throw new Exception("[ERROR] Error initializing file: " + filename, ioe);
        }
    }

    private static void printFileContent(String filename) throws Exception {
        // FILE_OUT
        System.out.println("FILE: " + filename);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            throw new Exception("[ERROR] Error reading file " + filename, ioe);
        }
    }

}
