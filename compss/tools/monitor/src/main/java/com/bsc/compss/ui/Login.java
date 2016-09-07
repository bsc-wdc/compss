package com.bsc.compss.ui;

import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zk.ui.Executions;
import com.bsc.compss.ui.Constants;
import com.bsc.compss.ui.auth.AuthenticationService;


public class Login {

    private String message = "";


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
