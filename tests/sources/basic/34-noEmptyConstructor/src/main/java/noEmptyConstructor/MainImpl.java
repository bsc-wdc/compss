package noEmptyConstructor;

import customObjectClasses.InvalidObject;
import customObjectClasses.ValidObject;


public class MainImpl {

    public static ValidObject validTask(ValidObject voIN, ValidObject voINOUT) {
        voINOUT.setName(voIN.getId());

        ValidObject retvo = new ValidObject(5);
        return retvo;
    }

    public static InvalidObject invalidTask(InvalidObject invoIN, InvalidObject invoINOUT) {
        invoINOUT.setName(invoIN.getId());

        InvalidObject retinvo = new InvalidObject(5);
        return retinvo;
    }

}
