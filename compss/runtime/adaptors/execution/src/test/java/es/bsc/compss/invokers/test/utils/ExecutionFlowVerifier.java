/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.invokers.test.utils.FakeInvocationContext.InvocationContextListener;
import es.bsc.compss.invokers.test.utils.FakeInvoker.InvokerListener;
import es.bsc.compss.invokers.test.objects.StorageTestObject;
import es.bsc.compss.invokers.test.objects.TestObject;
import es.bsc.compss.invokers.test.utils.types.Event;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAction;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAssertion;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import static org.junit.Assert.*;
import storage.StubItf;


/**
 *
 * @author flordan
 */
public class ExecutionFlowVerifier implements InvocationContextListener, InvokerListener {

    LinkedList<Event> expectedEvents = new LinkedList<>();

    public ExecutionFlowVerifier() {

    }

    public void add(Event.Type type, Object state, Object reply) {
        System.out.println("Expecting " + type + " for " + state);
        expectedEvents.add(new Event(type, state, reply));
    }

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

        LinkedList<InvocationParameterAssertion> asserts;
        asserts = (LinkedList<InvocationParameterAssertion>) e.getExpectedState();
        for (InvocationParameterAssertion assertion : asserts) {
            assertion.validate(inv);
        }

        LinkedList<InvocationParameterAction> actions;
        actions = (LinkedList<InvocationParameterAction>) e.getAttachedReply();

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
                            fail("Cannot apply action " + action.getAction() + " to return value. Test design failure!!");
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
                            fail("Cannot apply action " + action.getAction() + " to return value. Test design failure!!");
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
        List<Object> expectedResults = (List<Object>) e.getExpectedState();
        if (results.size() != expectedResults.size()) {
            fail("Unexpected number of results. Expecting " + expectedResults.size() + " and obtained " + results.size());
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
        assertEquals("Getting object " + rename + " when expecting getting " + e.getExpectedState(), e.getExpectedState(), rename);
        return e.getAttachedReply();
    }

    @Override
    public Object getPersistentObject(String id) {
        System.out.println("Getting persistent object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Getting persisted object " + id + " when no more events where expected", e);
        assertEquals("Getting persisted object when expecting " + e.getType(), e.getType(), Event.Type.GET_PERSISTENT_OBJECT);
        assertEquals("Getting persisted object " + id + " when expecting getting " + e.getExpectedState(), e.getExpectedState(), id);
        return e.getAttachedReply();
    }

    @Override
    public void storeObject(String rename, Object value) {
        System.out.println("storing object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Storing object " + rename + " when no more events where expected", e);
        assertEquals("Storing object when expecting " + e.getType(), e.getType(), Event.Type.STORE_OBJECT);
        assertEquals("Storing object " + rename + " when expecting storing " + e.getExpectedState(), e.getExpectedState(), rename);
        assertEquals("Storing value " + value + "for object " + rename + " when expecting getting" + e.getAttachedReply(),
                ((TestObject) value).getValue(), e.getAttachedReply());
    }

    @Override
    public void storePersistentObject(String id, Object obj) {
        System.out.println("storing persisted object");
        Event e = expectedEvents.pollFirst();
        assertNotNull("Storing persisted object " + id + " when no more events where expected", e);
        assertEquals("Storing persited object when expecting " + e.getType(), e.getType(), Event.Type.STORE_PERSISTENT_OBJECT);
        assertEquals("Storing persitend object " + id + " when expecting storing " + e.getExpectedState(), e.getExpectedState(), id);
        assertEquals("Storing value " + obj + "for persisted object " + id + " when expecting getting" + e.getAttachedReply(),
                ((TestObject) obj).getValue(), e.getAttachedReply());

    }

}
