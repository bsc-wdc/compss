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
package es.bsc.compss.types.data;

import es.bsc.compss.types.annotations.parameter.DataType;


public interface Transferable {

    /**
     * Returns the source data.
     *
     * @return The source data.
     */
    public Object getDataSource();

    /**
     * Sets the source data.
     *
     * @param dataSource New source data.
     */
    public void setDataSource(Object dataSource);

    /**
     * Returns the target data.
     *
     * @return The target data.
     */
    public String getDataTarget();

    /**
     * Sets the target data.
     *
     * @param target New target data.
     */
    public void setDataTarget(String target);

    /**
     * Returns the data Transfer type.
     *
     * @return The data Transfer type.
     */
    public DataType getType();

    /**
     * Return whether the source should be preserved on the transfer.
     *
     * @return {@literal true} if source is to be preserved, {@literal false} otherwise.
     */
    public boolean isSourcePreserved();

    /**
     * Returns whether the target can be changed on the transfer.
     *
     * @return {@literal false} if target is to be preserved, {@literal false} otherwise.
     */
    public boolean isTargetFlexible();

}
