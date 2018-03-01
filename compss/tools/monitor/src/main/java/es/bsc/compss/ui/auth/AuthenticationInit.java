package es.bsc.compss.ui.auth;

import org.zkoss.zk.ui.Executions;
import org.zkoss.zk.ui.Page;
import org.zkoss.zk.ui.util.Initiator;
import es.bsc.compss.ui.Constants;
import java.util.Map;


public class AuthenticationInit implements Initiator {

    @Override
    public void doInit(Page page, Map<String, Object> args) throws Exception {
        UserCredential cred = AuthenticationService.getUserCredential();

        if (cred == null || !cred.isAuthenticated()) {
            Executions.sendRedirect(Constants.LOGIN_PAGE);
            return;
        }
    }
}
