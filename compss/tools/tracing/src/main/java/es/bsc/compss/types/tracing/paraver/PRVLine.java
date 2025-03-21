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
package es.bsc.compss.types.tracing.paraver;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.tracing.SynchEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.tracing.EventTranslator;
import es.bsc.compss.util.tracing.ThreadTranslator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class PRVLine {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);

    // Info used for matching sync events
    private static final Integer SYNC_TYPE = TraceEventType.SYNC.code;
    private static final String SYNC_REGEX = "(^\\d+:\\d+:\\d+):(\\d+):(\\d+):(\\d+).*:" + SYNC_TYPE + ":(\\d+)";
    private static final Pattern SYNC_PATTERN = Pattern.compile(SYNC_REGEX);

    // Selectors for replace Pattern
    private static final Integer R_ID_INDEX = 1;
    private static final Integer TIMESTAMP_INDEX = 4;
    private static final Integer WORKER_ID_INDEX = 2;
    private static final Integer VALUE_INDEX = 5;

    // Positions in .prv when spliting the line on ':'
    public static final int EVENT_TYPE_POS = 0;
    public static final int EMISOR_CPU_ID_POS = 1;
    public static final int EMISOR_APP_POS = 2;
    public static final int EMISOR_TASK_POS = 3;
    public static final int EMISOR_THREAD_POS = 4;
    public static final int TIMESTAMP_POS = 5;

    protected String[] values;


    /**
     * Creates a PrvLine from the values composing a line of the prv file.
     *
     * @param values parsed values
     */
    private PRVLine(String[] values) {
        this.values = values;
    }

    /**
     * Parse a string to convert it to a PRVLine.
     * 
     * @param line string to be parsed
     * @return PRVLine containing the information parsed from the line
     */
    public static PRVLine parse(String line) {
        String[] values = line.split(":");
        PRVLine parsedLine;
        switch (values[0]) {
            case "1":
                parsedLine = new StatePRVLine(values);
                break;
            case "2":
                // State change
                parsedLine = new EventPRVLine(values);
                break;
            case "3":
                // Communication
                parsedLine = new CommPRVLine(values);
                break;
            default:
                // Unkonwn type
                parsedLine = null;
        }
        return parsedLine;
    }

    /**
     * Checks whether the line passed in as a parameter contains a synchronization event and , if it does, returns the
     * event.
     *
     * @param line prv line that may contain a synch event
     * @return synch event description, if contained; {@literal null}, otherwise.
     */
    public static final SynchEvent parseSynchEvent(String line) {
        SynchEvent event = null;
        Matcher m = SYNC_PATTERN.matcher(line);
        if (m.find()) {
            String workerId = m.group(WORKER_ID_INDEX);
            String resourceID = m.group(R_ID_INDEX);
            Long timestamp = Long.parseLong(m.group(TIMESTAMP_INDEX));
            Long value = Long.parseLong(m.group(VALUE_INDEX));

            event = new SynchEvent(resourceID, workerId, timestamp, value);
        }
        return event;
    }

    /**
     * Applies an offset to the CPUs of the record.
     * 
     * @param offset offset to apply to all CPUs in the line
     */
    public void applyCPUOffset(int offset) {
        values[EMISOR_CPU_ID_POS] = computeOffset(values[EMISOR_CPU_ID_POS], offset);
    }

    /**
     * Parses the thread identifier of a state line.
     *
     * @return the thread identifier of a state line.
     */
    public PRVThreadIdentifier getEmisorThreadIdentifier() {
        String machine = values[EMISOR_APP_POS];
        String task = values[EMISOR_TASK_POS];
        String thread = values[EMISOR_THREAD_POS];
        return new PRVThreadIdentifier(machine, task, thread);
    }

    /**
     * Sets values corresponding to the thread identifier with the values in the string separated by ":".
     */
    private void setEmisorThreadIdentifier(PRVThreadIdentifier id) {
        values[EMISOR_APP_POS] = id.getApp();
        values[EMISOR_TASK_POS] = id.getTask();
        values[EMISOR_THREAD_POS] = id.getThread();
    }

    /**
     * Returns the value assigned to a certain event.
     * 
     * @param type identifier of the requested event type
     * @return value in the line for that event
     */
    public String getEventValue(String type) {
        // Do nothing except if it is an Event line (overriden)
        return null;
    }

    /**
     * Transforms the events in the line of the group eventGroupIdentifier from the local index to a global index.
     *
     * @param eventGroupIdentifier event Id to be translated
     * @param translator values correspondence
     */
    public void translateEventsFromGroup(String eventGroupIdentifier, EventTranslator<String> translator) {
        // Do nothing except if it is an Event line (overriden)
    }

    /**
     * Updates the timestamp of the record.
     * 
     * @param offset time offset to apply to the current element
     */
    public void applyTimeOffset(long offset) {
        values[TIMESTAMP_POS] = computeOffset(values[TIMESTAMP_POS], offset);
    }

    /**
     * Translates de thread identifiers in the line according to translations.
     *
     * @param translator Thread translator
     */
    public void translateLineThreads(ThreadTranslator<PRVThreadIdentifier> translator) {
        PRVThreadIdentifier threadId = this.getEmisorThreadIdentifier();
        threadId = translator.getNewThreadId(threadId);
        if (threadId != null) {
            this.setEmisorThreadIdentifier(threadId);
        }
    }

    private static String computeOffset(String original, long offset) {
        return Long.toString(Long.parseLong(original) + offset);
    }

    @Override
    public String toString() {
        return String.join(":", values);
    }

    /**
     * Returns true if only if timestamp from this is smaller than timestamp from otherLine.
     *
     * @param otherLine line to compare with
     * @return {@literal true} if timestamp is earlier than otherline's timestamp
     */
    public boolean goesBefore(String otherLine) {
        if (otherLine == null) {
            return false;
        }
        return goesBefore(PRVLine.parse(otherLine));
    }

    /**
     * Returns true if only if timestamp from this is smaller than timestamp from otherLine.
     *
     * @param otherLine line to compare with
     * @return {@literal true} if timestamp is earlier than otherline's timestamp
     */
    public boolean goesBefore(PRVLine otherLine) {
        long timeA = new Long(this.values[TIMESTAMP_POS]);
        long timeB = new Long(otherLine.values[TIMESTAMP_POS]);
        if (timeA < timeB) {
            return true;
        }
        if (timeA > timeB) {
            return false;
        }
        int typeA = new Integer(this.values[EVENT_TYPE_POS]);
        int typeB = new Integer(otherLine.values[EVENT_TYPE_POS]);
        return typeA <= typeB;
    }


    private static class StatePRVLine extends PRVLine {

        private static final int END_TIMESTAMP_POS = 6;
        private static final int STATE_POS = 7;


        private StatePRVLine(String[] values) {
            super(values);
        }

        @Override
        public void applyTimeOffset(long offset) {
            super.applyTimeOffset(offset);
            values[END_TIMESTAMP_POS] = computeOffset(values[END_TIMESTAMP_POS], offset);
        }
    }

    private static class EventPRVLine extends PRVLine {

        // STATE_ variables apply to lines describing an state change prv event
        private static final int STATE_EVENTS_START_POS = 6;


        private EventPRVLine(String[] values) {
            super(values);
        }

        @Override
        public String getEventValue(String type) {
            String result = null;
            for (int i = STATE_EVENTS_START_POS; i < values.length; i += 2) {
                if (values[i].compareTo(type) == 0) {
                    result = values[i + 1];
                }
            }
            return result;
        }

        @Override
        public void translateEventsFromGroup(String eventGroupIdentifier, EventTranslator<String> translator) {
            for (int i = STATE_EVENTS_START_POS; i < values.length; i += 2) {
                if (eventGroupIdentifier.equals(values[i])) {
                    String eventValue = values[i + 1];
                    String globalValue = translator.translateEvent(eventValue);
                    values[i + 1] = globalValue;
                }
            }
        }
    }

    private static class CommPRVLine extends PRVLine {

        // COM_ variables apply to lines describing a comunication prv event
        private static final int COM_EMISSOR_END_TIMESTAMP_POS = 6;
        private static final int COM_RECIV_CPU_ID_POS = 7;
        private static final int COM_RECIV_MACHINE_POS = 8;
        private static final int COM_RECIV_RUNTIME_EXECUTOR_POS = 9;
        private static final int COM_RECIV_THREAD_NUMBER_POS = 10;
        private static final int COM_RECIV_START_TIMESTAMP_POS = 11;
        private static final int COM_REVIC_END_TIMESTAMP_POS = 12;
        private static final int COM_SIZE_POS = 13;
        private static final int COM_TAG_POS = 14;


        private CommPRVLine(String[] values) {
            super(values);
        }

        @Override
        public void applyCPUOffset(int offset) {
            super.applyCPUOffset(offset);
            values[COM_RECIV_CPU_ID_POS] = computeOffset(values[COM_RECIV_CPU_ID_POS], offset);
        }

        private PRVThreadIdentifier getComRecivThreadIdentifier() {
            String machine = values[COM_RECIV_MACHINE_POS];
            String task = values[COM_RECIV_RUNTIME_EXECUTOR_POS];
            String thread = values[COM_RECIV_THREAD_NUMBER_POS];
            return new PRVThreadIdentifier(machine, task, thread);
        }

        /**
         * Sets values corresponding to the thread identifier with the values in the string separated by ":".
         */
        private void setComRecivThreadIdentifier(PRVThreadIdentifier id) {
            values[COM_RECIV_MACHINE_POS] = id.getApp();
            values[COM_RECIV_RUNTIME_EXECUTOR_POS] = id.getTask();
            values[COM_RECIV_THREAD_NUMBER_POS] = id.getThread();
        }

        @Override
        public void translateLineThreads(ThreadTranslator<PRVThreadIdentifier> translator) {
            super.translateLineThreads(translator);

            PRVThreadIdentifier recivThreadId = this.getComRecivThreadIdentifier();
            recivThreadId = translator.getNewThreadId(recivThreadId);
            if (recivThreadId != null) {
                this.setComRecivThreadIdentifier(recivThreadId);
            }
        }

        @Override
        public void applyTimeOffset(long offset) {
            super.applyTimeOffset(offset);
            values[COM_EMISSOR_END_TIMESTAMP_POS] = computeOffset(values[COM_EMISSOR_END_TIMESTAMP_POS], offset);
            values[COM_RECIV_START_TIMESTAMP_POS] = computeOffset(values[COM_RECIV_START_TIMESTAMP_POS], offset);
            values[COM_REVIC_END_TIMESTAMP_POS] = computeOffset(values[COM_REVIC_END_TIMESTAMP_POS], offset);

            values[COM_EMISSOR_END_TIMESTAMP_POS] += offset;
            values[COM_RECIV_START_TIMESTAMP_POS] += offset;
            values[COM_REVIC_END_TIMESTAMP_POS] += offset;
        }
    }

}
