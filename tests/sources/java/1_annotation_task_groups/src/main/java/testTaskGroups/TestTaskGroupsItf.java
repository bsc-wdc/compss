package testTaskGroups;

import es.bsc.compss.types.annotations.task.Method;


public interface TestTaskGroupsItf {
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl", timeOut="3000")
    void timeOutTaskSlow();
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl", timeOut="3000")
    void timeOutTaskFast();
    
}
