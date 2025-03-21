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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;

import java.io.Serializable;


/**
 * A File Instance is identified by its file and version identifiers.
 */
public class DataInstanceId implements Serializable, Comparable<DataInstanceId> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Time stamp
    private static final String label;

    // Data instance identifier fields
    private int dataId;
    private int versionId;

    // Renaming for this data version
    private String renaming;

    // LogicalData associated to this data version
    private LogicalData data;

    static {
        String sign = System.getProperty(COMPSsConstants.EXEC_LABEL);
        if (sign == null || sign.isEmpty()) {
            sign = Long.toString(System.currentTimeMillis());
        }
        label = sign;
    }


    /**
     * Creates a new DataInstanceId for serialization.
     */
    public DataInstanceId() {
        // For serialization
    }

    /**
     * Creates a new DataInstanceId for data {@code dataId} and version {@code versionId}.
     *
     * @param dataId Data Id.
     * @param versionId Version Id.
     */
    public DataInstanceId(int dataId, int versionId) {
        this.dataId = dataId;
        this.versionId = versionId;
        this.renaming = "d" + dataId + "v" + versionId + "_" + label + ".IT";
        this.data = Comm.registerData(renaming);
    }

    /**
     * Returns the associated dataId.
     *
     * @return The associated dataId.
     */
    public int getDataId() {
        return dataId;
    }

    /**
     * Returns the associated versionId.
     *
     * @return The associated versionId.
     */
    public int getVersionId() {
        return versionId;
    }

    /**
     * Returns the associated renaming.
     *
     * @return The associated renaming.
     */
    public String getRenaming() {
        return renaming;
    }

    /**
     * Returns the LogicalData associated to this DataInstanceID.
     *
     * @return the LogicalData associated to this DataInstanceID.
     */
    public LogicalData getData() {
        return data;
    }

    /**
     * Returns the previous version renaming to the given {@code renaming}.
     *
     * @param renaming Current version renaming.
     * @return The previous version renaming of {@code renaming}.
     */
    public static String previousVersionRenaming(String renaming) {
        int dIdx = renaming.indexOf('d');
        int vIdx = renaming.indexOf('v');
        int tIndex = renaming.indexOf('_');
        if (vIdx == 1) {
            return null;
        }
        int dataId = Integer.parseInt(renaming.substring(dIdx + 1, vIdx));
        int previousVersion = Integer.parseInt(renaming.substring(vIdx + 1, tIndex)) - 1;
        return "d" + dataId + "v" + previousVersion + "_" + label + ".IT";
    }

    /**
     * Deletes the DataInstance.
     */
    public void delete() {
        Comm.removeData(this.getRenaming(), true);
    }

    // Comparable interface implementation
    @Override
    public int compareTo(DataInstanceId dId) {
        if (dId == null) {
            throw new NullPointerException();
        }

        // First compare file identifiers
        if (dId.dataId != this.dataId) {
            return dId.dataId - this.dataId;
        } else {
            // If same file identifier, compare version identifiers
            return dId.versionId - this.versionId;
        }
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof DataInstanceId) && (this.compareTo((DataInstanceId) o) == 0);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "d" + dataId + "v" + versionId;
    }

}
