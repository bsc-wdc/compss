package groupservice;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for setNumWorkers complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="setNumWorkers">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="numWorkers" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "setNumWorkers", propOrder = {
    "numWorkers"
})
public class SetNumWorkers {

    protected int numWorkers;

    /**
     * Gets the value of the numWorkers property.
     * 
     */
    public int getNumWorkers() {
        return numWorkers;
    }

    /**
     * Sets the value of the numWorkers property.
     * 
     */
    public void setNumWorkers(int value) {
        this.numWorkers = value;
    }

}
