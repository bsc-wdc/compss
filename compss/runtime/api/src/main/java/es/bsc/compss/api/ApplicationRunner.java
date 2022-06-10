/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.api;

import java.util.concurrent.Semaphore;


public interface ApplicationRunner {

    /**
     * Notifies the application runner that the application's main code cannot make no progress until further notice.
     */
    public void stalledApplication();

    /**
     * Notifies the application runner that the execution of the application's main is ready to continue.
     * 
     * @param sem element to notify when the runner is ready
     */
    public void readyToContinue(Semaphore sem);

}
