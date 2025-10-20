
package es.bsc.compss.test;

import es.bsc.compss.types.annotations.task.Method;


public interface DemoClassItf {

    @Method(declaringClass = "es.bsc.compss.test.DemoClass")
    void addDelay();

}
