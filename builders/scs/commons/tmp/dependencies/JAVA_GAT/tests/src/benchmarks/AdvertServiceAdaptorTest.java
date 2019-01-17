/*
 * Created on Oct 25, 2005
 */
package benchmarks;

import java.net.URISyntaxException;
import java.util.NoSuchElementException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.advert.AdvertService;
import org.gridlab.gat.advert.MetaData;
import org.gridlab.gat.io.File;


public class AdvertServiceAdaptorTest {

    public static void main(String[] args) {
        AdvertServiceAdaptorTest a = new AdvertServiceAdaptorTest();
        a.test(args[0], args[1]).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String host) {

        AdaptorTestResult adaptorTestResult = new AdaptorTestResult(adaptor,
                host);

        Preferences preferences = new Preferences();
        preferences.put("advertservice.adaptor.name", adaptor);

        AdvertService advertService = null;
        try {
            advertService = GAT.createAdvertService(preferences);
        } catch (GATObjectCreationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        MetaData meta = new MetaData();
        meta.put("version", "5.0");
        meta.put("price", "2000");
        adaptorTestResult.put("put object '1' at path 'a'", addTest(
                advertService, "1", meta, "a"));
        adaptorTestResult.put("getMetaData 'a'             ", getMetaDataTest(
                advertService, "a", meta));
        meta = new MetaData();
        meta.put("version", "5.0");
        meta.put("price", "50");
        adaptorTestResult.put("put object '2' at path 'b'", addTest(
                advertService, "2", meta, "b"));
        adaptorTestResult.put("test path 'a' (CORRECT)   ",
                getAdvertisableTest(advertService, "a", "1", true));
        adaptorTestResult.put("test path 'a' (INCORRECT) ",
                getAdvertisableTest(advertService, "a", "2", false));
        adaptorTestResult.put("test path 'b' (CORRECT)   ",
                getAdvertisableTest(advertService, "b", "2", true));
        adaptorTestResult.put("export                    ", exportTest(
                advertService, "any://" + host + "/exported-advert-database"));
        meta = new MetaData();
        meta.put("version", "9.1");
        meta.put("price", "50");
        adaptorTestResult.put("put object '3' at path 'c'", addTest(
                advertService, "3", meta, "c"));
        adaptorTestResult.put("test path 'c' (CORRECT)   ",
                getAdvertisableTest(advertService, "c", "3", true));
        adaptorTestResult.put("import                    ", importTest(
                advertService, "any://" + host + "/exported-advert-database"));
        adaptorTestResult.put("test path 'c' (INCORRECT) ",
                getAdvertisableTest(advertService, "c", null, true));
        meta = new MetaData();
        meta.put("price", "50");
        adaptorTestResult.put("find price=50             ", findTest(
                advertService, meta, new String[] { "/b" }, true));
        meta = new MetaData();
        meta.put("version", "5.0");
        adaptorTestResult.put("find version=5.0          ", findTest(
                advertService, meta, new String[] { "/b", "/a" }, true));
        adaptorTestResult.put("delete 'b'                ", deleteTest(
                advertService, "b"));
        adaptorTestResult.put("setPWD 'test'             ", setPWDTest(
                advertService, "/test"));
        adaptorTestResult.put("getPWD 'test'             ", getPWDTest(
                advertService, "/test"));

        return adaptorTestResult;

    }

    private AdaptorTestResultEntry addTest(AdvertService advert, String name,
            MetaData meta, String path) {
        long start = System.currentTimeMillis();
        try {
            advert.add(GAT.createFile(name), meta, path);
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry getMetaDataTest(AdvertService advert,
            String path, MetaData meta) {
        long start = System.currentTimeMillis();
        boolean result = false;
        try {
            MetaData metaData = advert.getMetaData(path);
            result = metaData.equals(meta);
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(result, (stop - start), null);
    }

    private AdaptorTestResultEntry deleteTest(AdvertService advert, String path) {
        long start = System.currentTimeMillis();
        try {
            advert.delete(path);
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry setPWDTest(AdvertService advert, String path) {
        long start = System.currentTimeMillis();
        try {
            advert.setPWD(path);
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry getPWDTest(AdvertService advert, String path) {
        long start = System.currentTimeMillis();
        String pwd = null;
        try {
            pwd = advert.getPWD();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(path.equals(pwd), (stop - start),
                null);
    }

    private AdaptorTestResultEntry getAdvertisableTest(AdvertService advert,
            String path, String correctValue, boolean expectedResult) {
        long start = System.currentTimeMillis();
        boolean correct = false;
        try {
            correct = ((File) advert.getAdvertisable(path)).getPath().equals(
                    correctValue);
        } catch(NoSuchElementException e) {
            correct = correctValue == null;
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(correct == expectedResult,
                (stop - start), null);
    }

    private AdaptorTestResultEntry findTest(AdvertService advert,
            MetaData meta, String[] correctValues, boolean expectedResult) {
        long start = System.currentTimeMillis();
        boolean correct = true;
        try {
            String[] results = advert.find(meta);
            if (results == null) {
                correct = correctValues == results;
            } else {
                for (String result : results) {
                    for (String correctValue : correctValues) {
                        if (correctValue.equals(result)) {
                            correct = true;
                            break;
                        }
                        correct = false;
                    }
                    if (!correct) {
                        break;
                    }
                }
            }
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(correct == expectedResult,
                (stop - start), null);
    }

    private AdaptorTestResultEntry exportTest(AdvertService advert,
            String exportLocation) {
        long start = System.currentTimeMillis();
        try {
            advert.exportDataBase(new URI(exportLocation));
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry importTest(AdvertService advert,
            String importLocation) {
        long start = System.currentTimeMillis();
        try {
            advert.importDataBase(new URI(importLocation));
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }
}
