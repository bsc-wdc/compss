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

package es.bsc.compss.invokers.test.utils.types;

import static org.junit.Assert.fail;

import es.bsc.compss.executor.types.ExternalTaskStatus;
import es.bsc.compss.executor.types.ParameterResult;
import es.bsc.compss.executor.types.ParameterResult.CollectiveResult;

import org.junit.Test;


public class TaskResultParsingTest {

    @Test
    public void testCollectionParsing() {
        String message =
            "END_TASK 11 0 1 27 [(13,3a3bfeb3-b1a8-4576-aba9-656843e475dc),(13,062f4afa-f4a9-4c1e-8925-3388baef3379),"
                + "(13,3a095734-ec8a-4794-9f6d-49a5446783d8),(13,8d9bfd63-c83b-4e51-b1e6-163e044355ed),"
                + "(13,41d6558f-9700-44a7-a7d0-6cc92172ace0),(13,c9a2c121-654a-4627-a64c-43d94e9afd9e),"
                + "(13,b5b47cd4-1312-4677-88cf-10759ec64cb1),(13,331ee170-cd53-4592-b609-cc0ae98db4d8),"
                + "(13,dc6cbde3-70b8-4510-a4bd-75f4a358deda),(13,e7494c6a-d770-48d1-9705-e594076fc7c3)]";
        String[] line = message.split(" ");
        ExternalTaskStatus ets = new ExternalTaskStatus(line);
        if (ets.getNumParameters() != 1) {
            fail("Could not find the collection");
        }
        ParameterResult pr = ets.getResults().get(0);
        if (!pr.isCollective()) {
            fail("Result expected to be a collection.");
        }
        if (((CollectiveResult) pr).size() != 10) {
            fail("Could not find 10 elements in the collection");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCollectionInCollectionParsing() {
        String message = "END_TASK 5 0 1 27 ["
            + "[(13,aecc04a4-c842-4ce5-9ed6-661a47de7760),(13,aecc04a4-c842-4ce5-9ed6-661a47de7761)]"
            + ",[(13,aecc04a4-c842-4ce5-9ed6-661a47de7762),(13,aecc04a4-c842-4ce5-9ed6-661a47de7763)]" + "]";
        String[] line = message.split(" ");
        ExternalTaskStatus ets = new ExternalTaskStatus(line);
        if (ets.getNumParameters() != 1) {
            fail("Could not find the collection");
        }

        ParameterResult pr = ets.getResults().get(0);
        if (!pr.isCollective()) {
            fail("Result expected to be a collection.");
        }
        if (((CollectiveResult) pr).size() != 2) {
            fail("Could not find 2 subcollections in the collection");
        } else {

            ParameterResult r0 = ((CollectiveResult) pr).getElements().get(0);
            if (!r0.isCollective()) {
                fail("First subresult expected to be a collection.");
            }
            if (((CollectiveResult) r0).size() != 2) {
                fail("Could not find 2 elements in the first subcollection of the collection");
            }

            ParameterResult r1 = ((CollectiveResult) pr).getElements().get(1);
            if (!r1.isCollective()) {
                fail("Second subresult expected to be a collection.");
            }
            if (((CollectiveResult) r1).size() != 2) {
                fail("Could not find 2 elements in the second subcollection of the collection");
            }
        }
    }

}
