package integratedtoolkit.types.monitor.test;

import static org.junit.Assert.assertNotNull;
import integratedtoolkit.types.monitor.jaxb.COMPSsStateType;
import integratedtoolkit.types.monitor.jaxb.ObjectFactory;

import java.io.File;
import java.io.StringWriter;
import java.net.URISyntaxException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class MonitorFileTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        /*
         * if (TMP_FILE.exists()){ TMP_FILE.delete(); }
         */
    }

    @Test
    public void checkMonitorValuesTest() throws URISyntaxException, JAXBException {

        File f = new File(MonitorFileTest.class.getResource("/monitor.xml").toURI());
        JAXBContext jbc = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        Unmarshaller um = jbc.createUnmarshaller();
        JAXBElement<?> element = (JAXBElement<?>) um.unmarshal(f);
        COMPSsStateType monitor = (COMPSsStateType) element.getValue();

        assertNotNull(monitor);
        assertNotNull(monitor.getTasksInfo());
        assertNotNull(monitor.getTasksInfo().getApplication());
        assertNotNull(monitor.getCoresInfo());
        assertNotNull(monitor.getCoresInfo().getCore());
        assertNotNull(monitor.getResourceInfo());
        assertNotNull(monitor.getResourceInfo().getResource());

        jbc = JAXBContext.newInstance(COMPSsStateType.class);
        Marshaller m = jbc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        ObjectFactory objFact = new ObjectFactory();
        StringWriter writer = new StringWriter();
        m.marshal(objFact.createCOMPSsState(monitor), writer);
        System.out.println("\nPRINTING MARSHALLED MONITOR:\n");
        System.out.println(writer.toString() + "\n");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        /*
         * if (TMP_FILE.exists()){ TMP_FILE.delete(); }
         */
    }

}