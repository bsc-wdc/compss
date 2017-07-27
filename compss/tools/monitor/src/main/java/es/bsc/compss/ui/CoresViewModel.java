package es.bsc.compss.ui;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;

import es.bsc.compss.commons.Loggers;


public class CoresViewModel {

    private List<Core> cores;
    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_TASKS);


    @Init
    public void init() {
        cores = new LinkedList<>();
    }

    public List<Core> getCores() {
        return new ListModelList<Core>(this.cores);
    }

    @Command
    @NotifyChange("cores")
    public void update(List<String[]> newCoreData) {
        logger.debug("Updating Tasks ViewModel...");
        // Erase all current resources
        cores.clear();

        // Import new resources
        for (String[] dc : newCoreData) {
            /*
             * Each entry on the newCoreData array is of the form: coreId, implId, signature, meanET, minET, maxET,
             * execCount
             */
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("   - CoreElement: ");
                for (String elem : dc) {
                    sb.append(elem).append(" ");
                }
                logger.debug(sb.toString());
            }

            // Check color
            int taskId = Integer.parseInt(dc[0]) + 1; // +1 To shift according to COLORS and tracing
            int colorId = taskId % Constants.CORE_COLOR_MAX;
            String color = File.separator + "images" + File.separator + "colors" + File.separator + colorId + ".png";

            // color, dc
            Core c = new Core(color, dc);
            cores.add(c);
        }
        logger.debug("Tasks ViewModel updated");
    }

    @Command
    @NotifyChange("cores")
    public void clear() {
        cores.clear();
    }

}
