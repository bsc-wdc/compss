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
package es.bsc.compss.ui;

import es.bsc.compss.commons.Loggers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;


public class RuntimeLogViewModel {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_RUNTIME_LOG);
    private static final String RUNTIME_LOG_NOT_SELECTED = "Application's runtime.log file not selected";

    private String runtimeLogPath;
    private int lastParsedLine;
    private String content;
    private String filter;


    /**
     * Initializes a new runtime.log view model.
     */
    @Init
    public void init() {
        this.runtimeLogPath = new String("");
        this.lastParsedLine = 0;
        this.content = new String(RUNTIME_LOG_NOT_SELECTED);
        this.filter = new String("");
    }

    public String getRuntimeLog() {
        return this.content;
    }

    public String getFilter() {
        return this.filter;
    }

    /**
     * Sets a new filter for the runtime.log content and updates the UI view.
     * 
     * @param filter Filter expression.
     */
    @Command
    @NotifyChange({ "runtimeLog",
        "filter" })
    public void setFilter(@BindingParam("filter") String filter) {
        this.filter = filter;
        this.lastParsedLine = 0;
        this.content = "";
    }

    /**
     * Updates the UI view.
     */
    @Command
    @NotifyChange({ "runtimeLog",
        "filter" })
    public void update() {
        if (!Properties.getBasePath().equals("")) {
            // Check if applicaction has changed
            String newPath = Properties.getBasePath() + File.separator + Constants.RUNTIME_LOG;
            if (!this.runtimeLogPath.equals(newPath)) {
                // Load new application
                this.runtimeLogPath = newPath;
                this.lastParsedLine = 0;
                this.content = "";
                this.filter = "";
            }
            // Parse
            LOGGER.debug("Parsing runtime.log file...");

            try (BufferedReader br = new BufferedReader(new FileReader(this.runtimeLogPath))) {
                StringBuilder sb = new StringBuilder("");
                String line = br.readLine();
                int i = 0;
                while (line != null) {
                    if (i > this.lastParsedLine) {
                        if (line.contains(filter)) {
                            sb.append(line).append("\n");
                        }
                    }
                    i = i + 1;
                    line = br.readLine();
                }
                this.content += sb.toString();
                this.lastParsedLine = i - 1;
            } catch (IOException ioe) {
                LOGGER.error("Cannot parse runtime.log file: " + this.runtimeLogPath, ioe);
            }
        } else {
            // Load default value
            this.clear();
        }
    }

    /**
     * Clears the UI view.
     */
    @Command
    @NotifyChange("runtimeLog")
    public void clear() {
        // Load default value
        this.runtimeLogPath = "";
        this.lastParsedLine = 0;
        this.content = RUNTIME_LOG_NOT_SELECTED;
        this.filter = "";
    }

}
