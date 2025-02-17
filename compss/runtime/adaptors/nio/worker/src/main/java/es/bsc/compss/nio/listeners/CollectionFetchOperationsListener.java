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
package es.bsc.compss.nio.listeners;

import es.bsc.compss.data.FetchDataListener;
import es.bsc.compss.data.MultiOperationFetchListener;


public class CollectionFetchOperationsListener extends MultiOperationFetchListener {

    private final String collectionDataId;
    private final FetchDataListener listener;


    public CollectionFetchOperationsListener(String collectionDataId, FetchDataListener listener) {
        this.collectionDataId = collectionDataId;
        this.listener = listener;
    }

    @Override
    public void doCompleted() {
        this.listener.fetchedValue(this.collectionDataId);
    }

    @Override
    public void doFailure(String failedDataId, Exception e) {
        this.listener.errorFetchingValue(this.collectionDataId, e);
    }
}
