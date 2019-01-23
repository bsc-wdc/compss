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
package es.bsc.compss.data;

import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import java.util.List;


public interface DataManager {

    public void init() throws InitializationException;

    public void stop();

    public String getStorageConf();

    public void removeObsoletes(List<String> obsoletes);

    public void fetchParam(InvocationParam param, int i, FetchDataListener tt);

    public void loadParam(InvocationParam param) throws Exception;

    public void storeParam(InvocationParam param);

    public Object getObject(String dataMgmtId);

    public void storeValue(String name, Object value);

    public void storeFile(String dataId, String string);


    public static interface FetchDataListener {

        public void fetchedValue();

    }
}
