package testPscoStreams;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;

import es.bsc.distrostreamlib.api.pscos.PscoDistroStream;

import model.Person;


public interface MainItf {

    @Constraints(processorArchitecture = "Intel")
    @Method(declaringClass = "testPscoStreams.Tasks")
    void writePscos(@Parameter(type = Type.STREAM, direction = Direction.OUT) PscoDistroStream<Person> pds,
        @Parameter() int sleepTime);

    @Constraints(processorArchitecture = "Intel")
    @Method(declaringClass = "testPscoStreams.Tasks")
    void writePscosList(@Parameter(type = Type.STREAM, direction = Direction.OUT) PscoDistroStream<Person> pds,
        @Parameter() int sleepTime);

    @Constraints(processorArchitecture = "AMD")
    @Method(declaringClass = "testPscoStreams.Tasks")
    Integer readPscos(@Parameter(type = Type.STREAM, direction = Direction.IN) PscoDistroStream<Person> pds,
        @Parameter() int sleepTime);

    @Method(declaringClass = "testPscoStreams.Tasks")
    Integer processPsco(@Parameter(type = Type.OBJECT, direction = Direction.IN) Person p);

}
