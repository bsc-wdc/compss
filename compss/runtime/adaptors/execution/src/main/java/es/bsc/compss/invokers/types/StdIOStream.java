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
package es.bsc.compss.invokers.types;

public class StdIOStream {

    private String stdIn = null;
    private String stdOut = null;
    private String stdErr = null;


    public StdIOStream() {
        // Nothing to do since all attributes have been initialized
    }

    public String getStdIn() {
        return stdIn;
    }

    public String getStdOut() {
        return stdOut;
    }

    public String getStdErr() {
        return stdErr;
    }

    public void setStdIn(String stdIn) {
        this.stdIn = stdIn;
    }

    public void setStdOut(String stdOut) {
        this.stdOut = stdOut;
    }

    public void setStdErr(String stdErr) {
        this.stdErr = stdErr;
    }

}
