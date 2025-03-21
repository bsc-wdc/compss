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

import es.bsc.compss.ui.Constants;
import es.bsc.compss.ui.auth.AuthenticationService;

import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zk.ui.Executions;


public class Login {

    private String message = "";


    /**
     * Performs the UI authentication of the given user {@code username}.
     * 
     * @param username Username to authenticate.
     */
    @Command
    @NotifyChange("message")
    public void authenticate(@BindingParam("username") String username) {
        if (!AuthenticationService.login(username)) {
            this.message = "Incorrect username";
            return;
        }
        this.message = "Welcome, " + username;
        Executions.sendRedirect(Constants.MAIN_PAGE);
    }

    public String getMessage() {
        return this.message;
    }

    @Command
    public void logout() {
        AuthenticationService.logout();
        Executions.sendRedirect(Constants.LOGIN_PAGE);
    }
}
