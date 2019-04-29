
package masterinworker;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface TestItf {

    @Method(declaringClass = "masterinworker.Tasks")
    void createFileWithContent(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Constraints(processorArchitecture = "master")
    @Method(declaringClass = "masterinworker.Tasks")
    void createFileWithContentMaster(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Constraints(processorArchitecture = "worker")
    @Method(declaringClass = "masterinworker.Tasks")
    void createFileWithContentWorker(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor01")
    @Method(declaringClass = "masterinworker.Tasks")
    void createFileWithContentWorker01(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor02")
    @Method(declaringClass = "masterinworker.Tasks")
    void createFileWithContentWorker02(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "masterinworker.Tasks")
    void checkFileWithContent(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Constraints(processorArchitecture = "master")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkFileWithContentMaster(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Constraints(processorArchitecture = "worker")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkFileWithContentWorker(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor01")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkFileWithContentWorker01(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor02")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkFileWithContentWorker02(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateFileWithContent(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Constraints(processorArchitecture = "master")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateFileWithContentMaster(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Constraints(processorArchitecture = "worker")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateFileWithContentWorker(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor01")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateFileWithContentWorker01(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor02")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateFileWithContentWorker02(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "masterinworker.Tasks")
    StringWrapper createObjectWithContent(
            @Parameter(type = Type.STRING) String content);

    @Constraints(processorArchitecture = "master")
    @Method(declaringClass = "masterinworker.Tasks")
    StringWrapper createObjectWithContentMaster(
            @Parameter(type = Type.STRING) String content);

    @Constraints(processorArchitecture = "worker")
    @Method(declaringClass = "masterinworker.Tasks")
    StringWrapper createObjectWithContentWorker(
            @Parameter(type = Type.STRING) String content);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor01")
    @Method(declaringClass = "masterinworker.Tasks")
    StringWrapper createObjectWithContentWorker01(
            @Parameter(type = Type.STRING) String content);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor02")
    @Method(declaringClass = "masterinworker.Tasks")
    StringWrapper createObjectWithContentWorker02(
            @Parameter(type = Type.STRING) String content);

    @Method(declaringClass = "masterinworker.Tasks")
    void checkObjectWithContent(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.OBJECT) StringWrapper sw);

    @Constraints(processorArchitecture = "master")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkObjectWithContentMaster(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.OBJECT) StringWrapper sw);

    @Constraints(processorArchitecture = "worker")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkObjectWithContentWorker(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.OBJECT) StringWrapper sw);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor01")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkObjectWithContentWorker01(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.OBJECT) StringWrapper sw);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor02")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkObjectWithContentWorker02(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.OBJECT) StringWrapper sw);

    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateObjectWithContent(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.OBJECT, direction = Direction.INOUT) StringWrapper sw);

    @Constraints(processorArchitecture = "master")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateObjectWithContentMaster(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.OBJECT, direction = Direction.INOUT) StringWrapper sw);

    @Constraints(processorArchitecture = "worker")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateObjectWithContentWorker(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.OBJECT, direction = Direction.INOUT) StringWrapper sw);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor01")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateObjectWithContentWorker01(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.OBJECT, direction = Direction.INOUT) StringWrapper sw);

    @Constraints(processorArchitecture = "worker", processorName = "MainProcessor02")
    @Method(declaringClass = "masterinworker.Tasks")
    void checkAndUpdateObjectWithContentWorker02(
            @Parameter(type = Type.STRING) String content,
            @Parameter(type = Type.STRING) String newContent,
            @Parameter(type = Type.OBJECT, direction = Direction.INOUT) StringWrapper sw);

}
