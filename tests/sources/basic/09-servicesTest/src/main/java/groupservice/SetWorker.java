package groupservice;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for setWorker complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="setWorker">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="worker" type="{http://groupService}person" minOccurs="0"/>
 *         &lt;element name="id" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "setWorker", propOrder = {
    "worker",
    "id"
})
public class SetWorker {

    protected Person worker;
    protected int id;

    /**
     * Gets the value of the worker property.
     * 
     * @return
     *     possible object is
     *     {@link Person }
     *     
     */
    public Person getWorker() {
        return worker;
    }

    /**
     * Sets the value of the worker property.
     * 
     * @param value
     *     allowed object is
     *     {@link Person }
     *     
     */
    public void setWorker(Person value) {
        this.worker = value;
    }

    /**
     * Gets the value of the id property.
     * 
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the value of the id property.
     * 
     */
    public void setId(int value) {
        this.id = value;
    }

}
