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

/**
 * Description of a task parameter. A parameter carries its data type, value and value class, its data management
 * identifiers, the URIs of its sources, the renaming information of the value, its weight and its STD IO stream usage.
 * Parameters with a collective data type are described through the {@link InvocationParamCollection} refinement.
 */
public interface InvocationParam {

    /**
     * Sets the parameter data type.
     *
     * @param type Parameter data type.
     */
    void setType(DataType type);

    /**
     * Returns the parameter data type.
     *
     * @return The parameter data type.
     */
    DataType getType();

    /**
     * Returns whether the parameter is a collective type.
     *
     * @return {@literal true} if the parameter is a collective type, {@literal false} otherwise.
     */
    boolean isCollective();

    /**
     * Returns whether the source data of the parameter must be preserved when storing the new value.
     *
     * @return {@literal true} if the source data must be preserved, {@literal false} otherwise.
     */
    boolean isPreserveSourceData();

    /**
     * Returns whether the final value of the parameter must be written when the execution ends.
     *
     * @return {@literal true} if the final value must be written, {@literal false} otherwise.
     */
    boolean isWriteFinalValue();

    /**
     * Returns the parameter prefix.
     *
     * @return The parameter prefix.
     */
    String getPrefix();

    /**
     * Returns the parameter name.
     *
     * @return The parameter name.
     */
    String getName();

    /**
     * Returns the parameter content type.
     *
     * @return The parameter content type.
     */
    String getContentType();

    /**
     * Returns whether the parameter renaming must be kept.
     *
     * @return {@literal true} if the renaming must be kept, {@literal false} otherwise.
     */
    boolean isKeepRename();

    /**
     * Returns the parameter weight.
     *
     * @return The parameter weight.
     */
    double getWeight();

    /**
     * Returns the STD IO stream usage of the parameter.
     *
     * @return The STD IO stream usage.
     */
    StdIOStream getStdIOStream();

    /**
     * Returns the original name of the parameter data.
     *
     * @return The original data name.
     */
    String getOriginalName();

    /**
     * Sets the original name of the parameter data.
     *
     * @param originalName The original data name.
     */
    void setOriginalName(String originalName);

    /**
     * Returns the renamed name of the parameter data.
     *
     * @return The renamed data name.
     */
    String getRenamedName();

    /**
     * Sets the renamed name of the parameter data.
     *
     * @param renamedName The renamed data name.
     */
    void setRenamedName(String renamedName);

    /**
     * Returns the current value of the parameter.
     *
     * @return The parameter value.
     */
    Object getValue();

    /**
     * Sets the current value of the parameter.
     *
     * @param val The parameter value.
     */
    void setValue(Object val);

    /**
     * Sets the class of the parameter value.
     *
     * @param aClass The class of the parameter value.
     */
    void setValueClass(Class<?> aClass);

    /**
     * Returns the class of the parameter value.
     *
     * @return The class of the parameter value.
     */
    Class<?> getValueClass();

    /**
     * Returns the data management identifier of the parameter.
     *
     * @return The data management identifier.
     */
    String getDataMgmtId();

    /**
     * Returns the identifier of the source data of the parameter.
     *
     * @return The source data identifier.
     */
    String getSourceDataId();

    /**
     * Returns the URIs where the source data of the parameter is located.
     *
     * @return The source data URIs.
     */
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
