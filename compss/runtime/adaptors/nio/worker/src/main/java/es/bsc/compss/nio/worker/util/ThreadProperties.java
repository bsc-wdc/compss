package es.bsc.compss.nio.worker.util;

import java.util.Properties;


public class ThreadProperties extends Properties {

    /**
     * Custom Serial ID
     */
    private static final long serialVersionUID = 3L;

    private final ThreadLocal<Properties> localProperties = new ThreadLocal<Properties>() {

        @Override
        protected Properties initialValue() {
            return new Properties();
        }
    };


    public ThreadProperties(Properties properties) {
        super(properties);
    }

    @Override
    public String getProperty(String key) {
        String localValue = this.localProperties.get().getProperty(key);
        if (localValue != null) {
            return localValue;
        }

        return super.getProperty(key);
    }

    @Override
    public Object setProperty(String key, String value) {
        return this.localProperties.get().setProperty(key, value);
    }
}