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
package es.bsc.compss.invokers.test.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import es.bsc.compss.invokers.test.objects.StorageTestObject;
import es.bsc.compss.invokers.test.objects.TestObject;
import es.bsc.compss.invokers.test.utils.FakeInvocationContext.InvocationContextListener;
import es.bsc.compss.invokers.test.utils.FakeInvoker.InvokerListener;
import es.bsc.compss.invokers.test.utils.types.Event;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAction;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAssertion;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import storage.StubItf;


public class ExecutionFlowVerifier implements InvocationContextListener, InvokerListener {

    private LinkedList<Event> expectedEvents = new LinkedList<>();


    public ExecutionFlowVerifier() {

    }

    public void add(Event.Type type, Object state, Object reply) {
        System.out.println("Expecting " + type + " for " + state);
        expectedEvents.add(new Event(type, state, reply));
    }

    /**
     * Test completed.
     */
    public void testCompleted() {
        System.out.println("Test Completed");
        if (!expectedEvents.isEmpty()) {
            fail("Test finished with pending events:" + expectedEvents);
        }
    }

    @Override
    public List<Object> runningMethod(Invocation inv) {
        System.out.println(" Running Method");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Running the method when no more events where expected", e);
        assertEquals("Running the method before expected", e.getType(), Event.Type.RUNNING_METHOD);

        @SuppressWarnings("unchecked")
        LinkedList<InvocationParameterAssertion> asserts =
            (LinkedList<InvocationParameterAssertion>) e.getExpectedState();
        for (InvocationParameterAssertion assertion : asserts) {
            assertion.validate(inv);
        }

        @SuppressWarnings("unchecked")
        LinkedList<InvocationParameterAction> actions = (LinkedList<InvocationParameterAction>) e.getAttachedReply();

        LinkedList<Object> results = new LinkedList<>();
        for (InvocationParameterAction action : actions) {
            switch (action.getRole()) {
                case RETURN:
                    switch (action.getAction()) {
                        case CREATE:
                            results.add(new TestObject((int) action.getValue()));
                            break;
                        case CREATE_PERSISTENT:
                            String id = (String) ((Object[]) action.getValue())[0];
                            int value = (int) ((Object[]) action.getValue())[1];
                            results.add(new StorageTestObject(id, value));
                            break;
                        default:
                            fail("Cannot apply action " + action.getAction()
                                + " to return value. Test design failure!!");
                    }
                    break;
                default:
                    InvocationParam param = action.obtain(inv);
                    switch (action.getAction()) {
                        case UPDATE:
                            TestObject to = (TestObject) param.getValue();
                            to.updateValue((int) action.getValue());
                            break;
                        case PERSIST:
                            StubItf si = (StubItf) param.getValue();
                            si.makePersistent((String) action.getValue());
                            break;
                        default:
                            fail("Cannot apply action " + action.getAction()
                                + " to return value. Test design failure!!");
                    }
            }
        }
        return results;
    }

    @Override
    public void methodReturn(List<Object> results) {
        System.out.println("Method executed");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Method completed when no more events where expected", e);
        assertEquals("Event of execution completed not expected yet", e.getType(), Event.Type.METHOD_RETURN);
        @SuppressWarnings("unchecked")
        List<Object> expectedResults = (List<Object>) e.getExpectedState();
        if (results.size() != expectedResults.size()) {
            fail("Unexpected number of results. Expecting " + expectedResults.size() + " and obtained "
                + results.size());
        }
        int resIdx = 0;
        Iterator<Object> resultsItr = results.iterator();
        for (Object expectedResult : expectedResults) {
            Object result = resultsItr.next();
            assertEquals("Unexpected value for result value #" + resIdx + " the method execution",
                ((TestObject) expectedResult).getValue(), ((TestObject) result).getValue());
            resIdx++;
        }
    }

    @Override
    public Object getObject(String rename) {
        System.out.println("Getting object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Getting object " + rename + " when no more events where expected", e);
        assertEquals("Getting object when expecting " + e.getType(), e.getType(), Event.Type.GET_OBJECT);
        assertEquals("Getting object " + rename + " when expecting getting " + e.getExpectedState(),
            e.getExpectedState(), rename);
        return e.getAttachedReply();
    }

    @Override
    public Object getPersistentObject(String id) {
        System.out.println("Getting persistent object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Getting persisted object " + id + " when no more events where expected", e);
        assertEquals("Getting persisted object when expecting " + e.getType(), e.getType(),
            Event.Type.GET_PERSISTENT_OBJECT);
        assertEquals("Getting persisted object " + id + " when expecting getting " + e.getExpectedState(),
            e.getExpectedState(), id);
        return e.getAttachedReply();
    }

    @Override
    public void storeObject(String rename, Object value) {
        System.out.println("storing object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Storing object " + rename + " when no more events where expected", e);
        assertEquals("Storing object when expecting " + e.getType(), e.getType(), Event.Type.STORE_OBJECT);
        assertEquals("Storing object " + rename + " when expecting storing " + e.getExpectedState(),
            e.getExpectedState(), rename);
        assertEquals(
            "Storing value " + value + "for object " + rename + " when expecting getting" + e.getAttachedReply(),
            ((TestObject) value).getValue(), e.getAttachedReply());
    }

    @Override
    public void storePersistentObject(String id, Object obj) {
        System.out.println("storing persisted object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Storing persisted object " + id + " when no more events where expected", e);
        assertEquals("Storing persited object when expecting " + e.getType(), e.getType(),
            Event.Type.STORE_PERSISTENT_OBJECT);
        assertEquals("Storing persitend object " + id + " when expecting storing " + e.getExpectedState(),
            e.getExpectedState(), id);
        assertEquals(
            "Storing value " + obj + "for persisted object " + id + " when expecting getting" + e.getAttachedReply(),
            ((TestObject) obj).getValue(), e.getAttachedReply());

    }

}
