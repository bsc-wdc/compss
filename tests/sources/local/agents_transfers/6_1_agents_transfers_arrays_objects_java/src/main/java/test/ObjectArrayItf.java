package test;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface ObjectArrayItf {

    // @Method(declaringClass = "test.ObjectArray")
    // void consumeObject(
    // @Parameter(direction = Direction.IN) Obj obj
    // );

    @Method(declaringClass = "test.ObjectArray")
    Integer[][] in_return(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matA);

    @Method(declaringClass = "test.ObjectArray")
    Integer[][] in_return_w_print(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matA);

    @Method(declaringClass = "test.ObjectArray")
    Integer[][] nested_in_return(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matA,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    void nested_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    void inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

    @Method(declaringClass = "test.ObjectArray")
    void inout_w_print(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

    @Method(declaringClass = "test.ObjectArray")
    void print_task(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    Integer[][] nested_generation_return();

    @Method(declaringClass = "test.ObjectArray")
    Integer[][] generation_return();

    @Method(declaringClass = "test.ObjectArray")
    void consumption(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    void nested_generation_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

    @Method(declaringClass = "test.ObjectArray")
    void generation_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

}
