package nonNativeTasks;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.annotations.task.Binary;


public interface MainItf {
    
    /*
     * Simple tasks with return statements
     */
    @Binary(binary = "${OUT_ERR_BINARY}")
    void simpleTask1(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );

    @Binary(binary = "${OUT_ERR_BINARY}")
    int simpleTask2(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );
    
    @Binary(binary = "${OUT_ERR_BINARY}")
    Integer simpleTask3(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );
    
    /*
     * Simple tasks with prefixes
     */
    @Binary(binary = "${OUT_ERR_BINARY}")
    void taskWithPrefix(
        @Parameter(type = Type.STRING, direction = Direction.IN, prefix="--message=") String message
    );

    /*
     * Tasks with STDIN redirection
     */
    @Binary(binary = "${IN_OUT_ERR_BINARY}")
    void taskSTDINFileRedirection(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.IN, stream = Stream.STDIN) String fileIn
    );

    @Binary(binary = "${IN_OUT_ERR_BINARY}")
    int taskSTDINFileRedirectionWithEV(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.IN, stream = Stream.STDIN) String fileIn
    );

    /*
     * Tasks with STDOUT redirection
     */
    @Binary(binary = "${OUT_ERR_BINARY}")
    void taskSTDOUTFileRedirection(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
    );

    @Binary(binary = "${OUT_ERR_BINARY}")
    int taskSTDOUTFileRedirectionWithEV(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
    );

    /*
     * Tasks with STDOUT redirection in append mode
     */
    @Binary(binary = "${OUT_ERR_BINARY}")
    void taskSTDOUTFileRedirectionAppend(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.INOUT, stream = Stream.STDOUT) String fileOut
    );

    @Binary(binary = "${OUT_ERR_BINARY}")
    int taskSTDOUTFileRedirectionWithEVAppend(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.INOUT, stream = Stream.STDOUT) String fileOut
    );

    /*
     * Tasks with STDERR redirection
     */
    @Binary(binary = "${OUT_ERR_BINARY}")
    void taskSTDERRFileRedirection(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String fileErr
    );

    @Binary(binary = "${OUT_ERR_BINARY}")
    int taskSTDERRFileRedirectionWithEV(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String fileErr
    );

    /*
     * Tasks with STDERR redirection in append mode
     */
    @Binary(binary = "${OUT_ERR_BINARY}")
    void taskSTDERRFileRedirectionAppend(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.INOUT, stream = Stream.STDERR) String fileErr
    );

    @Binary(binary = "${OUT_ERR_BINARY}")
    int taskSTDERRFileRedirectionWithEVAppend(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.INOUT, stream = Stream.STDERR) String fileErr
    );

    /*
     * Full tasks
     */
    @Binary(binary = "${IN_OUT_ERR_BINARY}")
    void fullTask1(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.IN, stream = Stream.STDIN) String fileIn,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String fileErr
    );

    @Binary(binary = "${IN_OUT_ERR_BINARY}")
    int fullTask2(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.IN, stream = Stream.STDIN) String fileIn,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String fileErr
    );
    
}
