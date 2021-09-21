package testHttpTask;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.HTTP;


public interface TestHttpTaskItf {

    // GET REQUEST TESTS
    @HTTP(serviceName = "service_1", resource = "dummy/", request = "GET", declaringClass = "testHttpTask.TestHttpTaskImpl")
    void testGet();

    @HTTP(serviceName = "service_1", resource = "get_length/{{message}}", request = "GET", declaringClass = "testHttpTask.TestHttpTaskImpl")
    int testGetLength(@Parameter(name = "message") String message);

    @HTTP(serviceName = "service_1", resource = "print_message/{{message}}", request = "GET", declaringClass = "testHttpTask.TestHttpTaskImpl")
    String testProducesString(@Parameter(name = "message") String message);

    @HTTP(serviceName = "service_1", resource = "produce_format/{{message}}", request = "GET", declaringClass = "testHttpTask.TestHttpTaskImpl", produces = "{'child_json':{'depth_1':'one','message':'{{return_0}}'},'depth_0':'zero'}")
    String testNestedProduces(@Parameter(name = "message") String message);

    @HTTP(serviceName = "service_1", resource = "post/", request = "POST", declaringClass = "testHttpTask.TestHttpTaskImpl", payload = "payload")
    String testPost();

    @HTTP(serviceName = "service_1", resource = "post_json/", request = "POST", declaringClass = "testHttpTask.TestHttpTaskImpl", payload = "{{message}}")
    String testPayloadWithParam(@Parameter(name = "message") String message);

    @HTTP(serviceName = "service_1", resource = "post_json/", request = "POST", declaringClass = "testHttpTask.TestHttpTaskImpl", payload = "{{payload}}")
    String testPayloadWithFileParam(@Parameter(name = "payload", type = Type.FILE) String payload);
}
