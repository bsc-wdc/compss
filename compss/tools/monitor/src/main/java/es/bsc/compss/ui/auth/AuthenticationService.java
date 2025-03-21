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
package es.bsc.compss.ui.auth;

import es.bsc.compss.commons.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.zk.ui.Session;
import org.zkoss.zk.ui.Sessions;


public class AuthenticationService {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_AUTHENTICATION);


    public static UserCredential getUserCredential() {
        Session session = Sessions.getCurrent();
        return (UserCredential) session.getAttribute("userCredential");
    }

    /**
     * Authenticate the given username {@code username}.
     * 
     * @param username Username.
     * @return Whether it has been authenticated or not.
     */
    public static boolean login(String username) {
        LOGGER.info("Login");
        // Create user credential
        UserCredential cred = new UserCredential(username);

        // Verify user
        if (cred.setAuthenticated()) {
            // Correct user. Add to session
            Session session = Sessions.getCurrent();
            session.setAttribute("userCredential", cred);
            // Successfull login
            return true;
        }

        // Invalid user
        return false;
    }

    /**
     * Logout the currently logged user.
     */
    public static void logout() {
        LOGGER.info("Logout");
        Session session = Sessions.getCurrent();
        session.removeAttribute("userCredential");
    }
}
