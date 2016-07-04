package integratedtoolkit.connectors.rocci.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import integratedtoolkit.connectors.rocci.types.json.JSONResources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class ROCCIClientTest {
	
	@Test
	public void actionDescribeTest() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		try {
				String jsonInput = readFile(new File(ROCCIClientTest.class.getResource("/describe.json").toURI()));
				jsonInput = "{\"resources\":"+jsonInput+"}";
	
				//convert the json string back to object		
				JSONResources obj = gson.fromJson(jsonInput, JSONResources.class);
				
				String IP = null;
				String network_state = null;
				
				//System.out.println(" - Compute State: "+obj.getResources().get(0).getAttributes().getOcci().getCompute().getState());
				
				for(int i = 0; i< obj.getResources().get(0).getLinks().size(); i++){
				  if(obj.getResources().get(0).getLinks().get(i).getAttributes().getOcci().getNetworkinterface() != null){
				     IP = obj.getResources().get(0).getLinks().get(i).getAttributes().getOcci().getNetworkinterface().getAddress();
				     network_state = obj.getResources().get(0).getLinks().get(i).getAttributes().getOcci().getNetworkinterface().getState();
				     break;
				  }
				}
				
				assertNotNull(obj.getResources().get(0));
				assertEquals("Should be a resources defined", obj.getResources().size(), 1);
				assertNotNull(obj.getResources().get(0).getAttributes());
				assertNotNull(obj.getResources().get(0).getAttributes().getOcci());
				assertNotNull(obj.getResources().get(0).getAttributes().getOcci().getCompute());
				assertNotNull(obj.getResources().get(0).getAttributes().getOcci().getCompute().getState());
				assertEquals("Compute state should be active", obj.getResources().get(0).getAttributes().getOcci().getCompute().getState(), "active");
				
				assertNotNull(obj.getResources().get(0).getLinks());
				assertTrue( obj.getResources().get(0).getLinks().size() > 0);
				
				assertEquals("Network state should be active", network_state, "active");
				assertNotNull(IP);
				
				//System.out.println(" - Network State: "+network_state);
				//System.out.println(" - IP Address: "+IP);
	 		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	private String readFile( File file ) throws IOException {
	    BufferedReader reader = new BufferedReader( new FileReader (file));
	    
	    String line = null;
	    StringBuilder stringBuilder = new StringBuilder();
	    String ls = System.getProperty("line.separator");
	    try {
		    while( ( line = reader.readLine() ) != null ) {
		        stringBuilder.append(line);
		        stringBuilder.append(ls);
		    }
	    } catch (IOException e) {
	    	throw e;
	    } finally {
	    	reader.close();
	    }

	    return stringBuilder.toString();
	}

} //End test class
