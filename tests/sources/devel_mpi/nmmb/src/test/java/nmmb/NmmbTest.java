package nmmb;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.junit.Test;


public class NmmbTest {

    // Test Logger
    private static final Logger LOGGER = LogManager.getLogger("Console");


    @Test
    public void dateParsing() {
        final SimpleDateFormat STR_TO_DATE = new SimpleDateFormat("yyyyMMdd");
        final long ONE_DAY_IN_SECONDS = 1 * 24 * 60 * 60;

        // Date to convert
        String inputDateSTR = "20140901";

        // Load date
        Date date = null;
        try {
            date = STR_TO_DATE.parse(inputDateSTR);
        } catch (ParseException pe) {
            LOGGER.error("[ERROR] Cannot parse date", pe);
            LOGGER.error("Aborting...");
            System.exit(1);
        }

        // Increase date by one day
        LOGGER.info("Start : " + STR_TO_DATE.format(date));
        date = Date.from(date.toInstant().plusSeconds(ONE_DAY_IN_SECONDS));
        LOGGER.info("End   : " + STR_TO_DATE.format(date));
    }

    @Test
    public void dateShow() {
        final SimpleDateFormat STR_TO_DATE = new SimpleDateFormat("yyyyMMdd");
        final SimpleDateFormat YEAR = new SimpleDateFormat("yyyy");
        final SimpleDateFormat MONTH = new SimpleDateFormat("MM");
        final SimpleDateFormat DAY = new SimpleDateFormat("dd");
        final SimpleDateFormat NAME = new SimpleDateFormat("ddMMMyyyy");

        // Date to convert
        String inputDateSTR = "20140901";

        // Load date
        Date date = null;
        try {
            date = STR_TO_DATE.parse(inputDateSTR);
        } catch (ParseException pe) {
            LOGGER.error("[ERROR] Cannot parse date", pe);
            LOGGER.error("Aborting...");
            System.exit(1);
        }

        LOGGER.info(" FULL   : " + STR_TO_DATE.format(date));
        LOGGER.info(" YEAR   : " + YEAR.format(date));
        LOGGER.info(" MONTH  : " + MONTH.format(date));
        LOGGER.info(" DAY    : " + DAY.format(date));
        LOGGER.info(" NAME   : " + NAME.format(date));
    }
}
