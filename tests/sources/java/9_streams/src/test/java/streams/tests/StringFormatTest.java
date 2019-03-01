package streams.tests;

import static org.junit.Assert.assertEquals;

import java.util.Locale;

import org.junit.Test;


public class StringFormatTest {

    @Test
    public void stringFormat() {
        String msg = String.format(Locale.ROOT, "{\"type\":\"end\", \"t\":%.3f, \"k\":%d}", 123.45, 0);
        String formattedMsg = "{\"type\":\"end\", \"t\":123.450, \"k\":0}";
        
        assertEquals(msg, formattedMsg);
    }

}
