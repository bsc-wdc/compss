/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.util.types;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.tracing.ThreadIdentifier;
import es.bsc.compss.util.tracing.Threads;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PrvLine {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);

    // Positions in .prv when spliting the line on ':'
    public static final int EVENT_TYPE = 0;
    public static final int TIMESTAMP_POS = 5;
    // STATE_ variables apply to lines describing an state change prv event
    public static final int STATE_MACHINE_POS = 2;
    public static final int STATE_RUNTIME_EXECUTOR_POS = 3;
    public static final int STATE_THREAD_NUMBER_POS = 4;
    public static final int STATE_EVENTS_START_POS = 6;
    // COM_ variables apply to lines describing a comunication prv event
    public static final int COM_SEND_MACHINE_POS = 2;
    public static final int COM_SEND_RUNTIME_EXECUTOR_POS = 3;
    public static final int COM_SEND_THREAD_NUMBER_POS = 4;
    public static final int COM_RECIV_MACHINE_POS = 8;
    public static final int COM_RECIV_RUNTIME_EXECUTOR_POS = 9;
    public static final int COM_RECIV_THREAD_NUMBER_POS = 10;

    private String[] values;


    /**
     * Creates a PrvHeader with the parsed data from the string.
     * 
     * @param line line to parse
     */
    public PrvLine(String line) {
        this.values = line.split(":");
    }

    public void setAgentNumber(String s) {
        this.values[STATE_MACHINE_POS] = s;
    }

    /**
     * Parses the thread identifier of a state line.
     * 
     * @return the thread identifier of a state line.
     */
    public ThreadIdentifier getStateLineThreadIdentifier() {
        String machine = values[STATE_MACHINE_POS];
        String task = values[STATE_RUNTIME_EXECUTOR_POS];
        String thread = values[STATE_THREAD_NUMBER_POS];
        return new ThreadIdentifier(machine, task, thread);
    }

    /**
     * Sets values corresponding to the thread identifier with the values in the string separated by ":".
     */
    private void setStateLineThreadIdentifier(ThreadIdentifier id) {
        values[STATE_MACHINE_POS] = id.getApp();
        values[STATE_RUNTIME_EXECUTOR_POS] = id.getTask();
        values[STATE_THREAD_NUMBER_POS] = id.getThread();
    }

    private ThreadIdentifier getComSenderThreadIdentifier() {
        String machine = values[COM_SEND_MACHINE_POS];
        String task = values[COM_SEND_RUNTIME_EXECUTOR_POS];
        String thread = values[COM_SEND_THREAD_NUMBER_POS];
        return new ThreadIdentifier(machine, task, thread);
    }

    /**
     * Sets values corresponding to the thread identifier with the values in the string separated by ":".
     */
    private void setComSenderThreadIdentifier(ThreadIdentifier id) {
        values[COM_SEND_MACHINE_POS] = id.getApp();
        values[COM_SEND_RUNTIME_EXECUTOR_POS] = id.getTask();
        values[COM_SEND_THREAD_NUMBER_POS] = id.getThread();
    }

    private ThreadIdentifier getComRecivThreadIdentifier() {
        String machine = values[COM_RECIV_MACHINE_POS];
        String task = values[COM_RECIV_RUNTIME_EXECUTOR_POS];
        String thread = values[COM_RECIV_THREAD_NUMBER_POS];
        return new ThreadIdentifier(machine, task, thread);
    }

    /**
     * Sets values corresponding to the thread identifier with the values in the string separated by ":".
     */
    private void setComRecivThreadIdentifier(ThreadIdentifier id) throws Exception {
        values[COM_RECIV_MACHINE_POS] = id.getApp();
        values[COM_RECIV_RUNTIME_EXECUTOR_POS] = id.getTask();
        values[COM_RECIV_THREAD_NUMBER_POS] = id.getThread();
    }

    /**
     * Returns a map with the events of the line being the event groups the keys and the values of the events the values
     * of the map.
     */
    public Map<String, String> getEvents() {
        Map<String, String> result = new HashMap<String, String>();
        if ("3".equals(values[0])) {
            // is comunication line
            return result;
        }
        for (int i = STATE_EVENTS_START_POS; i < values.length; i += 2) {
            result.put(values[i], values[i + 1]);
        }
        return result;
    }

    /**
     * Transforms the events in the line of the group eventGroupIdentifier from the local index to a global index.
     * 
     * @param localIndex must contain the map from a value of the event to a common identifier (common between the two
     *            maps)
     * @param globalIndex must contain the map from the aforementioned common identifier to the global value of this
     *            identifier
     */
    public void translateLineToGlobalIndex(String eventGroupIdentifier, Map<String, String> globalIndex,
        Map<String, String> localIndex) {
        for (int i = STATE_EVENTS_START_POS; i < values.length; i += 2) {
            if (eventGroupIdentifier.equals(values[i])) {
                String eventValue = values[i + 1];
                String eventIdentifier = localIndex.get(eventValue);
                String globalValue = globalIndex.get(eventIdentifier);
                values[i + 1] = globalValue;
            }
        }
    }

    public String toString() {
        return String.join(":", values);
    }

    /**
     * Translates de thread identifiers in the line acording to translations.
     */
    public void translateLineThreads(Map<ThreadIdentifier, ThreadIdentifier> translations) throws Exception {
        if (!"3".equals(this.values[0])) {
            // the line is an state change event
            ThreadIdentifier oldThreadId = this.getStateLineThreadIdentifier();
            if (translations.containsKey(oldThreadId)) {
                this.setStateLineThreadIdentifier(translations.get(oldThreadId));
            }
        } else {
            // line is a comunication
            ThreadIdentifier senderThreadId = this.getComSenderThreadIdentifier();
            if (translations.containsKey(senderThreadId)) {
                this.setComSenderThreadIdentifier(translations.get(senderThreadId));
            }
            ThreadIdentifier recivThreadId = this.getComRecivThreadIdentifier();
            if (translations.containsKey(recivThreadId)) {
                this.setComRecivThreadIdentifier(translations.get(recivThreadId));
            }
        }
    }

    /**
     * Returns true if only if timestramp from this is smaller than timestamp from otherLine.
     */
    public boolean goesBefore(String otherLine) throws Exception {
        if (otherLine == null) {
            return false;
        }
        return goesBefore(new PrvLine(otherLine));
    }

    /**
     * Returns true if only if this timestamp from this is smaller than timestamp from otherLine.
     */
    public boolean goesBefore(PrvLine otherLine) {
        long timeA = new Long(this.values[TIMESTAMP_POS]);
        long timeB = new Long(otherLine.values[TIMESTAMP_POS]);
        if (timeA < timeB) {
            return true;
        }
        if (timeA > timeB) {
            return false;
        }
        int typeA = new Integer(this.values[EVENT_TYPE]);
        int typeB = new Integer(otherLine.values[EVENT_TYPE]);
        return typeA <= typeB;
    }

}
