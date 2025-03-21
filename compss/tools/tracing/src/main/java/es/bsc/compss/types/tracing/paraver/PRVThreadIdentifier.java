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

import es.bsc.compss.types.tracing.ThreadIdentifier;
import java.util.Objects;


public class PRVThreadIdentifier implements ThreadIdentifier {

    private String app;
    private String task;
    private String thread;


    /**
     * Constructs a new Thread Identifier.
     *
     * @param app App tag of the thread Id
     * @param task task tag of the thread Id
     * @param thread thread number
     */
    public PRVThreadIdentifier(int app, int task, int thread) {
        this(Integer.toString(app), Integer.toString(task), Integer.toString(thread));
    }

    /**
     * Constructs a new Thread Identifier.
     * 
     * @param app App tag of the thread Id
     * @param task task tag of the thread Id
     * @param thread thread number
     */
    public PRVThreadIdentifier(String app, String task, String thread) {
        super();
        this.app = app;
        this.task = task;
        this.thread = thread;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getTask() {
        return task;
    }

    public void setTask(String task) {
        this.task = task;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    @Override
    public String toString() {
        return app + ":" + task + ":" + thread;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PRVThreadIdentifier other = (PRVThreadIdentifier) obj;
        if (!Objects.equals(this.app, other.app)) {
            return false;
        }
        if (!Objects.equals(this.task, other.task)) {
            return false;
        }
        if (!Objects.equals(this.thread, other.thread)) {
            return false;
        }
        return true;
    }

    @Override
    public int getIdAtLevel(int level) {
        switch (level) {
            case 0:
                return Integer.parseInt(app);
            case 1:
                return Integer.parseInt(task);
            case 2:
                return Integer.parseInt(thread);
            default:
                return -1;
        }
    }

}
