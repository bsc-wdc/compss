package recursive;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.task.Method;


public interface CountItf {

    @Method(declaringClass = "recursive.Count")
    int count(@Parameter int count);

}
