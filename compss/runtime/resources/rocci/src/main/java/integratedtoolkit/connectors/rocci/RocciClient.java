package integratedtoolkit.connectors.rocci;

import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.rocci.types.json.JSONResources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class RocciClient {

    private static Logger LOGGER = null;

    private String cmd_line = "";
    private String attributes = "";


    public RocciClient(List<String> cmd_string, String attr, Logger logger) {
        LOGGER = logger;

        for (String s : cmd_string) {
            cmd_line += s + " ";
        }

        attributes = attr;
    }

    public String describe_resource(String resource_id) throws ConnectorException {
        // EXAMPLE DESCRIBE WITHOUT VOMS:
        // occi --endpoint https://machine.defaultdomain:11443 --output-format json_extended_pretty \
        // --auth x509 --ca-path /etc/grid-security/certificates --user-cred /home/user/certs/pmes.pem \
        // --password=XXXX --action describe --resource /compute/2414

        // EXAMPLE DESCRIBE WITH VOMS:
        // occi --endpoint https://machine.defaultdomain:11443 --output-format json_extended_pretty \
        // --voms --auth x509 --ca-path /etc/grid-security/certificates --user-cred /home/user/certs/pmes.pem \
        // --password=XXXX --action describe --resource /compute/2414

        // OUTPUT:
        /*
         * { "resources":[ { "kind":"http://schemas.ogf.org/occi/infrastructure#compute", "mixins":[ ... ], "actions":[
         * ... ], "attributes":{ "occi":{ "core":{ "id":"2414", "title":"test-storage-vm",
         * "summary":"Instantiated with rOCCI-server on Fri, 25 Jul 2014 15:25:10 +0000." }, "compute":{
         * "architecture":"x64", "cores":1, "memory":2.0, "speed":1.0, "state":"active" } }, "org":{ ... } },
         * "id":"2414", "links":[ ... ] } ] }
         */

        String res_desc = "";
        String cmd = cmd_line + "--action describe" + " --resource " + resource_id;

        // System.out.println("Describe resource CMD -> "+cmd);

        try {
            res_desc = execute_cmd(cmd);
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }
        return res_desc;

    }

    public String get_resource_status(String resource_id) throws ConnectorException {
        String res_status = null;
        String jsonOutput = describe_resource(resource_id);

        /*
         * String [] splitted; splitted = res_desc.split(System.getProperty("line.separator"));
         * 
         * for(String s:splitted){ if(s.indexOf("STATE:")!=-1) { res_status =
         * s.substring(s.indexOf("STATE")+6).replaceAll("\\s", ""); //System.out.println("res_status -> "+ res_status);
         * } }
         */

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        jsonOutput = "{\"resources\":" + jsonOutput + "}";

        // convert the json string back to object
        JSONResources obj = gson.fromJson(jsonOutput, JSONResources.class);
        res_status = obj.getResources().get(0).getAttributes().getOcci().getCompute().getState();

        return res_status;
    }

    public String get_resource_address(String resource_id) throws ConnectorException {
        String res_ip = null;
        String jsonOutput = describe_resource(resource_id);

        /*
         * String [] splitted; String [] ip_splitted;
         * 
         * splitted = res_desc.split(System.getProperty("line.separator")); for(String s:splitted){
         * if(s.indexOf("IP ADDRESS:")!=-1) { res_ip = s.substring(s.indexOf("IP ADDRESS")+11).replaceAll("\\s", "");
         * ip_splitted = res_ip.split("\\."); if(ip_splitted.length > 0) if(ip_splitted[0].compareTo("10") != 0 &&
         * ip_splitted[0].compareTo("192") !=0){ return res_ip; } } }
         */

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        jsonOutput = "{\"resources\":" + jsonOutput + "}";

        // convert the json string back to object
        JSONResources obj = gson.fromJson(jsonOutput, JSONResources.class);

        for (int i = 0; i < obj.getResources().get(0).getLinks().size(); i++) {
            if (obj.getResources().get(0).getLinks().get(i).getAttributes().getOcci().getNetworkinterface() != null) {
                res_ip = obj.getResources().get(0).getLinks().get(i).getAttributes().getOcci().getNetworkinterface().getAddress();
                // network_state =
                // obj.getResources().get(0).getLinks().get(i).getAttributes().getOcci().getNetworkinterface().getState();
                break;
            }
        }

        return res_ip;
    }

    public void delete_compute(String resource_id) {
        // COMMAND:
        // occi --endpoint https://machine.defaultdomain:11443 --auth x509 --output-format json_extended_pretty \
        // --ca-path /etc/grid-security/certificates --user-cred /home/user/certs/pmes.pem --password=XXXX --action delete \
        // --resource /compute/2421

        // OUTPUT:
        // void

        String cmd = cmd_line + "--action delete" + " --resource " + resource_id;
        // System.out.println("Delete resource CMD -> "+cmd);

        try {
            execute_cmd(cmd);
        } catch (ConnectorException e) {
            LOGGER.error(e);
        } catch (InterruptedException e) {
            LOGGER.error(e);
        } catch (Exception e) {
            LOGGER.error(e);
        }

    }

    public String create_compute(String os_tpl, String resource_tpl) {
        // COMMAND:
        // occi --endpoint https://machine.defaultdomain:11443 --auth x509 --output-format json_extended_pretty \
        // --ca-path /etc/grid-security/certificates --user-cred /home/user/certs/pmes.pem --password=XXXX --action create \
        // --resource compute -M os_tpl#uuid_alyadan_test_61 -M resource_tpl#small --attribute
        // occi.core.title="test-storage-vm"

        // OUTPUT:
        // https://machine.defaultdomain:11443/compute/2423

        String s = "";

        String cmd = cmd_line + " --action create" + " --resource compute -M os_tpl#" + os_tpl + " -M resource_tpl#" + resource_tpl
                + " --attribute occi.core.title=\"" + attributes + "\"";

        // System.out.println("Create resource CMD -> "+cmd);

        try {
            s = execute_cmd(cmd);
        } catch (ConnectorException e) {
            LOGGER.error(e);
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }

        return s;
    }

    private String execute_cmd(String cmd_args) throws ConnectorException, InterruptedException {
        String return_string = "";
        String buff = null;

        String[] cmd_line = { "/bin/bash", "-c", "occi " + cmd_args };
        try {
            Process p = Runtime.getRuntime().exec(cmd_line);
            p.waitFor();
            if (p.exitValue() != 0) {
                throw new ConnectorException("Error executing command: \n occi " + cmd_args);
            }
            BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((buff = is.readLine()) != null) {
                // return_string += buff +"\n";
                return_string += buff;
            }

            return return_string;

        } catch (IOException e) {
            throw new ConnectorException(e);
        }
    }

}
