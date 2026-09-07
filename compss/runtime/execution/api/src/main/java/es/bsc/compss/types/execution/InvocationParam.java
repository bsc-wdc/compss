/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.semantics.data.DataType;
import es.bsc.compss.semantics.task.parameter.StdIOStream;
import java.util.List;

public interface InvocationParam {

    void setType(DataType type);

    DataType getType();

    boolean isCollective();

    boolean isPreserveSourceData();

    boolean isWriteFinalValue();

    String getPrefix();

    String getName();

    String getContentType();

    boolean isKeepRename();

    double getWeight();

    StdIOStream getStdIOStream();

    String getOriginalName();

    void setOriginalName(String originalName);

    String getRenamedName();

    void setRenamedName(String renamedName);

    Object getValue();

    void setValue(Object val);

    void setValueClass(Class<?> aClass);

    Class<?> getValueClass();

    String getDataMgmtId();

    String getSourceDataId();

    List<? extends InvocationParamURI> getSources();

    /**
     * Marks that the result for the parameter is not computed by the task itself but from one nested task.
     */
    void resultIsForwarded();

    /**
     * Returns whether the result for the parameter is computed by the task itself or by one nested task.
     * 
     * @return {@literal true} if the result is computed by a nested task; {@literal false}, if the task computed the
     *         value.
     */
    boolean isForwardedResult();

}
