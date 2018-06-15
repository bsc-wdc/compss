/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import java.io.PrintStream;
import storage.StorageException;


public interface InvocationContext {

    public String getHostName();

    public String getAppDir();

    public String getInstallDir();

    public PrintStream getThreadOutStream();

    public PrintStream getThreadErrStream();

    public Object getObject(String rename);

    public Object getPersistentObject(String id) throws StorageException;

    public void storeObject(String renaming, Object value);

    public void storePersistentObject(String id, Object obj);

}
