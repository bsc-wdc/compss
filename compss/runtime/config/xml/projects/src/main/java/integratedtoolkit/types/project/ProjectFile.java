package integratedtoolkit.types.project;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import integratedtoolkit.types.project.jaxb.CloudProviderType;
import integratedtoolkit.types.project.jaxb.CloudType;
import integratedtoolkit.types.project.jaxb.DataNodeType;
import integratedtoolkit.types.project.jaxb.ImageListType;
import integratedtoolkit.types.project.jaxb.ImageType;
import integratedtoolkit.types.project.jaxb.InstancesListTypes;
import integratedtoolkit.types.project.jaxb.ObjectFactory;
import integratedtoolkit.types.project.jaxb.ProjectType;
import integratedtoolkit.types.project.jaxb.ResourceType;
import integratedtoolkit.types.project.jaxb.WorkerType;


public class ProjectFile {

	ProjectType project;
	
	public ProjectFile(){
		project = (new ObjectFactory()).createProjectType();
	}
	
	public ProjectFile(File file) throws JAXBException{
		Unmarshaller um = getJAXBContext().createUnmarshaller();
		project = ((JAXBElement<ProjectType>) um.unmarshal(file)).getValue();
	}
	
	public ProjectFile(String xmlString) throws JAXBException{
		Unmarshaller um = getJAXBContext().createUnmarshaller();
		project = ((JAXBElement<ProjectType>) um.unmarshal(new StringReader(xmlString))).getValue();
	}
	
	public void toFile(File file) throws JAXBException{
		Marshaller m = getJAXBContext().createMarshaller();
		ObjectFactory objFact = new ObjectFactory();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		m.marshal(objFact.createProject(project), file);
	}
	
	public String getString() throws JAXBException{
		Marshaller m = getJAXBContext().createMarshaller();
		ObjectFactory objFact = new ObjectFactory();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		StringWriter sw = new StringWriter();
		m.marshal(objFact.createProject(project), sw);
		return sw.getBuffer().toString();
	}
	
	private static JAXBContext getJAXBContext() throws JAXBException{
		return JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
	}
	
	public ProjectType getProjectType(){
		return project;
	}
	
	public CloudType getCloud(){
		List<Object> objList = project.getDataNodeOrWorkerOrCloud();
		if (objList!=null && objList.size()>0){
			for (Object ob:objList){
				if (ob instanceof CloudType){
					return (CloudType)ob;
				}
			}
		}
		CloudType cloud = new CloudType();
		project.getDataNodeOrWorkerOrCloud().add(cloud);
		return cloud;
	}
	
	public List<CloudProviderType> getCloudProviders(){
		return getCloud().getProvider();
	}
	
	public CloudProviderType getCloudProvider(String providerName) throws Exception{
		List<CloudProviderType> objList = getCloud().getProvider();
		if (objList!=null && objList.size()>0){
			for (CloudProviderType cp:objList){
				if (cp.getName().equals(providerName))
					return cp;
			}
		}
		throw new Exception("Provider "+ providerName + " not found");
	}
	
	public CloudProviderType addCloudProvider(String providerName){
		CloudProviderType provider = new CloudProviderType();
		provider.setName(providerName);
		getCloud().getProvider().add(provider);
		return provider;
	}
	public ImageType addImageToProvider(String providerName, String imageId, String user,
			String workingDir, String installDir){
		CloudProviderType provider;
		try{
			provider = getCloudProvider(providerName);
		}catch (Exception e){
			provider = addCloudProvider(providerName);
		}
		return addImageToProvider(provider, imageId, user, workingDir, installDir);
	}
	
	public ImageType addImageToProvider(CloudProviderType provider, String imageId, String user,
			String workingDir, String installDir){
		ImageListType imgList = provider.getImageList();
		if (imgList==null){
			imgList = new ImageListType();
			provider.setImageList(imgList);
		}
		ImageType img = new ImageType();
		img.setName(imageId);
		img.setUser(user);
		img.setWorkingDir(workingDir);
		img.setInstallDir(installDir);
		imgList.getImage().add(img);
		return img;
	}
	
	public ResourceType addResourceTypeToProvider(String providerName, String resourceId){
		CloudProviderType provider;
		try{
			provider = getCloudProvider(providerName);
		}catch (Exception e){
			provider = addCloudProvider(providerName);
		}
		return addResourceTypeToProvider(provider, resourceId);
	}
	
	public ResourceType addResourceTypeToProvider(CloudProviderType provider, String resourceId){
		InstancesListTypes resList = provider.getInstanceTypes();
		if (resList==null){
			resList = new InstancesListTypes();
			provider.setInstanceTypes(resList);
		}
		ResourceType res = new ResourceType();
		res.setName(resourceId);
		resList.getResource().add(res);
		return res;
	}
	
	public List<DataNodeType> getDataNodes(){
		ArrayList<DataNodeType> list = new ArrayList<DataNodeType>();
		List<Object> objList = project.getDataNodeOrWorkerOrCloud();
		if (objList!=null && objList.size()>0){
			for (Object ob:objList){
				if (ob instanceof DataNodeType){
					list.add((DataNodeType)ob);
				}
			}
		}
		return list;
	}
	
	public DataNodeType getDataNode(String host) throws Exception{
		List<Object> objList = project.getDataNodeOrWorkerOrCloud();
		if (objList!=null && objList.size()>0){
			for (Object ob:objList){
				if (ob instanceof DataNodeType){
					if (((DataNodeType)ob).getHost().equals(host))
						return (DataNodeType)ob;
				}
			}
		}
		throw new Exception("DataNode "+ host + " not found");
	}
	
	public DataNodeType addDataNode(String host, String path, String protocol, 
			String user) throws Exception{
		DataNodeType dn = new DataNodeType();
		if (host != null)
			dn.setHost(host);
		else
			throw new Exception("Host data node should be defined");
		if (path != null)
			dn.setPath(path);
		if(protocol != null)
			dn.setProtocol(protocol);
		if (user != null)
			dn.setUser(user);
		project.getDataNodeOrWorkerOrCloud().add(dn);
		return dn;
	}
	
	public List<WorkerType> getWorkers(){
		ArrayList<WorkerType> list = new ArrayList<WorkerType>();
		List<Object> objList = project.getDataNodeOrWorkerOrCloud();
		if (objList!=null && objList.size()>0){
			for (Object ob:objList){
				if (ob instanceof WorkerType){
					list.add((WorkerType)ob);
				}
			}
		}
		return list;
	}
	
	public WorkerType addWorker(String name, String appDir, String installDir, 
			String workingDir, String user, Integer maxTasks) throws Exception{
		WorkerType worker = new WorkerType();
		if (name!=null)
			worker.setName(name);
		else
			throw new Exception("Worker name should be defined");
		if (installDir!=null)
			worker.setInstallDir(installDir);
		if (appDir!=null)
			worker.setAppDir(appDir);
		if (workingDir!=null)
			worker.setWorkingDir(workingDir);
		if (user!=null)
			worker.setUser(user);
		if (maxTasks!=null)
		worker.setLimitOfTasks(maxTasks);
		project.getDataNodeOrWorkerOrCloud().add(worker);
		return worker;
	}
	
	public WorkerType getWorker(String name) throws Exception{
		List<Object> objList = project.getDataNodeOrWorkerOrCloud();
		if (objList!=null && objList.size()>0){
			for (Object ob:objList){
				if (ob instanceof WorkerType){
					if (((WorkerType)ob).getName().equals(name))
						return (WorkerType)ob;
				}
			}
		}
		throw new Exception("Worker "+ name + " not found");
	}
	
	
	
}
