/** @file 
 *  Copyright 2002-2014 Barcelona Supercomputing Center (www.bsc.es)
 *  Life Science Department, 
 *  Computational Genomics Group (http://www.bsc.es/life-sciences/computational-genomics)
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
 *
 *  Last update: $LastChangedDate: 2015-01-08 10:46:38 +0100 (Thu, 08 Jan 2015) $
 *  Revision Number: $Revision: 13 $
 *  Last revision  : $LastChangedRevision: 13 $
 *  Written by     : Friman Sanchez C.
 *                 : friman.sanchez@gmail.com
 *  Modified by    :
 *                
 *  Guidance web page: http://cg.bsc.es/guidance/
 */

package guidance.utils;

/**
 * @brief GenericClass. This clase includes 5 attributes
 * @author Friman Sanchez
 * @date 2014-10-15
 */
public class GenericFile {

    private String dir = null;
    private String name = null;
    private String finalStatus = null;
    private String fullName = null;
    private String generatedBy = null;


    /**
     * A first constructor for GenericFile class
     * 
     * @param myDir
     * @param myName
     * @param myFinalStatus
     */
    public GenericFile(String myDir, String myName, String myFinalStatus) {
        dir = myDir;
        name = myName;
        finalStatus = myFinalStatus;
        fullName = myDir + "/" + myName;
        generatedBy = null;
    }

    /**
     * A second constructor for GenericFile class
     * 
     * @param myDir
     * @param myName
     * @param myFinalStatus
     * @param myGeneratedBy
     */
    public GenericFile(String myDir, String myName, String myFinalStatus, String myGeneratedBy) {
        dir = myDir;
        name = myName;
        finalStatus = myFinalStatus;
        fullName = myDir + "/" + myName;
        generatedBy = myGeneratedBy;

        // System.out.println("------------------------------------");
        // System.out.println("[GenericFile] : " + fullName);
        // System.out.println("[GenericFile] : " + finalStatus);
    }

    /**
     * A method to set dir field
     * 
     * @param myDir
     */
    public void setDir(String myDir) {
        dir = myDir;
    }

    /**
     * A method to set name field
     * 
     * @param myName
     */
    public void setName(String myName) {
        name = myName;
    }

    /**
     * A method to set finalStatus field
     * 
     * @param myFinalStatus
     */
    public void setFinalStatus(String myFinalStatus) {
        finalStatus = myFinalStatus;
    }

    /**
     * A method to set fullName field
     * 
     * @param myFullName
     */
    public void setFullName(String myFullName) {
        fullName = myFullName;
    }

    /**
     * A method to get dir field
     * 
     * @return
     */
    public String getDir() {
        return dir;
    }

    /**
     * A method to get Name field
     * 
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * A method to get finalStatus field
     * 
     * @return
     */
    public String getFinalStatus() {
        return finalStatus;
    }

    /**
     * A method to get fullName field
     * 
     * @return
     */
    public String getFullName() {
        return fullName;
    }

    /**
     * A method to get generatedBy field
     * 
     * @return
     */
    public String getGeneratedBy() {
        return generatedBy;
    }

}
