package com.bsc.compss.ui.auth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.zk.ui.Session;
import org.zkoss.zk.ui.Sessions;

import com.bsc.compss.commons.Loggers;


public class AuthenticationService {

    private static final Logger logger = LogManager.getLogger(Loggers.UI_AUTHENTICATION);


    public static UserCredential getUserCredential() {
        Session session = Sessions.getCurrent();
        return (UserCredential) session.getAttribute("userCredential");
    }

    public static boolean login(String username) {
        logger.info("Login");
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

    public static void logout() {
        logger.info("Logout");
        Session session = Sessions.getCurrent();
        session.removeAttribute("userCredential");
    }
}