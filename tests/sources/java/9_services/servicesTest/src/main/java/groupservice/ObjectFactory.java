package groupservice;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the groupservice package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _GetWorkerResponse_QNAME = new QName("http://groupService", "getWorkerResponse");
    private final static QName _GetOwner_QNAME = new QName("http://groupService", "getOwner");
    private final static QName _GetNumWorkers_QNAME = new QName("http://groupService", "getNumWorkers");
    private final static QName _ProductivityResponse_QNAME = new QName("http://groupService", "productivityResponse");
    private final static QName _SetWorkerResponse_QNAME = new QName("http://groupService", "setWorkerResponse");
    private final static QName _GetWorker_QNAME = new QName("http://groupService", "getWorker");
    private final static QName _SetNumWorkers_QNAME = new QName("http://groupService", "setNumWorkers");
    private final static QName _SetWorker_QNAME = new QName("http://groupService", "setWorker");
    private final static QName _SetNumWorkersResponse_QNAME = new QName("http://groupService", "setNumWorkersResponse");
    private final static QName _Productivity_QNAME = new QName("http://groupService", "productivity");
    private final static QName _GetNumWorkersResponse_QNAME = new QName("http://groupService", "getNumWorkersResponse");
    private final static QName _GetOwnerResponse_QNAME = new QName("http://groupService", "getOwnerResponse");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: groupservice
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link GetOwner }
     * 
     */
    public GetOwner createGetOwner() {
        return new GetOwner();
    }

    /**
     * Create an instance of {@link Productivity }
     * 
     */
    public Productivity createProductivity() {
        return new Productivity();
    }

    /**
     * Create an instance of {@link SetWorkerResponse }
     * 
     */
    public SetWorkerResponse createSetWorkerResponse() {
        return new SetWorkerResponse();
    }

    /**
     * Create an instance of {@link GetNumWorkers }
     * 
     */
    public GetNumWorkers createGetNumWorkers() {
        return new GetNumWorkers();
    }

    /**
     * Create an instance of {@link SetWorker }
     * 
     */
    public SetWorker createSetWorker() {
        return new SetWorker();
    }

    /**
     * Create an instance of {@link GetNumWorkersResponse }
     * 
     */
    public GetNumWorkersResponse createGetNumWorkersResponse() {
        return new GetNumWorkersResponse();
    }

    /**
     * Create an instance of {@link Person }
     * 
     */
    public Person createPerson() {
        return new Person();
    }

    /**
     * Create an instance of {@link GetWorkerResponse }
     * 
     */
    public GetWorkerResponse createGetWorkerResponse() {
        return new GetWorkerResponse();
    }

    /**
     * Create an instance of {@link SetNumWorkers }
     * 
     */
    public SetNumWorkers createSetNumWorkers() {
        return new SetNumWorkers();
    }

    /**
     * Create an instance of {@link SetNumWorkersResponse }
     * 
     */
    public SetNumWorkersResponse createSetNumWorkersResponse() {
        return new SetNumWorkersResponse();
    }

    /**
     * Create an instance of {@link ProductivityResponse }
     * 
     */
    public ProductivityResponse createProductivityResponse() {
        return new ProductivityResponse();
    }

    /**
     * Create an instance of {@link GetOwnerResponse }
     * 
     */
    public GetOwnerResponse createGetOwnerResponse() {
        return new GetOwnerResponse();
    }

    /**
     * Create an instance of {@link GetWorker }
     * 
     */
    public GetWorker createGetWorker() {
        return new GetWorker();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetWorkerResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "getWorkerResponse")
    public JAXBElement<GetWorkerResponse> createGetWorkerResponse(GetWorkerResponse value) {
        return new JAXBElement<GetWorkerResponse>(_GetWorkerResponse_QNAME, GetWorkerResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetOwner }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "getOwner")
    public JAXBElement<GetOwner> createGetOwner(GetOwner value) {
        return new JAXBElement<GetOwner>(_GetOwner_QNAME, GetOwner.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetNumWorkers }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "getNumWorkers")
    public JAXBElement<GetNumWorkers> createGetNumWorkers(GetNumWorkers value) {
        return new JAXBElement<GetNumWorkers>(_GetNumWorkers_QNAME, GetNumWorkers.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ProductivityResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "productivityResponse")
    public JAXBElement<ProductivityResponse> createProductivityResponse(ProductivityResponse value) {
        return new JAXBElement<ProductivityResponse>(_ProductivityResponse_QNAME, ProductivityResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SetWorkerResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "setWorkerResponse")
    public JAXBElement<SetWorkerResponse> createSetWorkerResponse(SetWorkerResponse value) {
        return new JAXBElement<SetWorkerResponse>(_SetWorkerResponse_QNAME, SetWorkerResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetWorker }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "getWorker")
    public JAXBElement<GetWorker> createGetWorker(GetWorker value) {
        return new JAXBElement<GetWorker>(_GetWorker_QNAME, GetWorker.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SetNumWorkers }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "setNumWorkers")
    public JAXBElement<SetNumWorkers> createSetNumWorkers(SetNumWorkers value) {
        return new JAXBElement<SetNumWorkers>(_SetNumWorkers_QNAME, SetNumWorkers.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SetWorker }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "setWorker")
    public JAXBElement<SetWorker> createSetWorker(SetWorker value) {
        return new JAXBElement<SetWorker>(_SetWorker_QNAME, SetWorker.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SetNumWorkersResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "setNumWorkersResponse")
    public JAXBElement<SetNumWorkersResponse> createSetNumWorkersResponse(SetNumWorkersResponse value) {
        return new JAXBElement<SetNumWorkersResponse>(_SetNumWorkersResponse_QNAME, SetNumWorkersResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Productivity }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "productivity")
    public JAXBElement<Productivity> createProductivity(Productivity value) {
        return new JAXBElement<Productivity>(_Productivity_QNAME, Productivity.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetNumWorkersResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "getNumWorkersResponse")
    public JAXBElement<GetNumWorkersResponse> createGetNumWorkersResponse(GetNumWorkersResponse value) {
        return new JAXBElement<GetNumWorkersResponse>(_GetNumWorkersResponse_QNAME, GetNumWorkersResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetOwnerResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://groupService", name = "getOwnerResponse")
    public JAXBElement<GetOwnerResponse> createGetOwnerResponse(GetOwnerResponse value) {
        return new JAXBElement<GetOwnerResponse>(_GetOwnerResponse_QNAME, GetOwnerResponse.class, null, value);
    }

}
