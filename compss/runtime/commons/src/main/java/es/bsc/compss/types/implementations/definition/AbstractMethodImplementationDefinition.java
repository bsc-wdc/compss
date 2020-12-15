package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;

import java.util.List;


public interface AbstractMethodImplementationDefinition extends ImplementationDefinition {

    public TaskType getTaskType();

    public MethodType getMethodType();

    public String toMethodDefinitionFormat();

    /**
     * Method to append AbstractMethodDefinition properties to an arguments list.
     * 
     * @param args arguments list
     * @param auxParam auxiliar parameter to pass in order to customize an argument according to the remote node (path,
     *            default value,..)
     */
    public void appendToArgs(List<String> args, String auxParam);

}
