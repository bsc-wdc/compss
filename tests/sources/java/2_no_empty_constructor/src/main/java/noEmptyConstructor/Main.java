package noEmptyConstructor;

import customObjectClasses.InvalidObject;
import customObjectClasses.ValidObject;


public class Main {

    public static void main(String[] args) {

        //----------------------------------------------------------------------------
        // Launch a valid task
        System.out.println("[LOG] Begin valid task test");
        
        ValidObject voIN = new ValidObject(1);
        ValidObject voINOUT = new ValidObject(2);
        ValidObject retvo = MainImpl.validTask(voIN, voINOUT);
        
        System.out.println("[RES_VALID] IN_ID = " + voIN.getId());
        System.out.println("[RES_VALID] RETURN_ID = " + retvo.getId());
        System.out.println("[RES_VALID] INOUT_ID = " + voINOUT.getId());
        System.out.println("[RES_VALID] INOUT_NAME = " + voINOUT.getName());
        

        //----------------------------------------------------------------------------
        // Launch a invalid task
        System.out.println("[LOG] Begin invalid task test");
        
        InvalidObject invoIN = new InvalidObject(1);
        InvalidObject invoINOUT = new InvalidObject(2);
        InvalidObject inretvo = MainImpl.invalidTask(invoIN, invoINOUT);
        
        System.out.println("[RES_INVALID] IN_ID = " + invoIN.getId());
        System.out.println("[RES_INVALID] RETURN_ID = " + inretvo.getId());
        System.out.println("[RES_INVALID] INOUT_ID = " + invoINOUT.getId());
        System.out.println("[RES_INVALID] INOUT_NAME = " + invoINOUT.getName());
    }

}
