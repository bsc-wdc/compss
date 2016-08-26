package integratedtoolkit.connectors.vmm;

import java.util.ArrayList;


public class VMs {

	private ArrayList<VMRequest> vms;


	public VMs() {
		setVms(new ArrayList<VMRequest>());
	}

	public ArrayList<VMRequest> getVms() {
		return vms;
	}

	public void setVms(ArrayList<VMRequest> vms) {
		this.vms = vms;
	}

}
