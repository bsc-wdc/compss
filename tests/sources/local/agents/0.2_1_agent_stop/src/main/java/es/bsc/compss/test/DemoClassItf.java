
package es.bsc.compss.test;

import es.bsc.compss.types.annotations.task.Method;


public interface DemoClassItf {

    @Method(declaringClass = "demo.DemoClass")
    void addDelay();

}
