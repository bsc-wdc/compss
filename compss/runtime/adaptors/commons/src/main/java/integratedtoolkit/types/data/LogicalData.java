package integratedtoolkit.types.data;

import integratedtoolkit.comm.Comm;
import static integratedtoolkit.comm.Comm.appHost;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.location.DataLocation.Type;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.operation.Copy;
import integratedtoolkit.types.data.operation.SafeCopyListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.SharedDiskManager;

import java.io.File;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

public class LogicalData {

	// Logical data name
	protected final String name;

	// Value in memory, null if value in disk
	protected Object value;
	protected boolean onFile;

	// List of existing copies
	protected final TreeSet<DataLocation> locations = new TreeSet<DataLocation>();
	private Semaphore removeHostLock = new Semaphore(1);		// Semaphore to avoid host deletion when file is beeing transfered
	private boolean isBeingSaved = false;
	
	// List of copies in progress
	private static final ConcurrentHashMap<String, LinkedList<CopyInProgress>> inProgress = new ConcurrentHashMap<String, LinkedList<CopyInProgress>>();
	private static final TreeMap<Resource, HashSet<LogicalData>> hostToPrivateFiles = new TreeMap<Resource, HashSet<LogicalData>>();
	private static final TreeMap<String, HashSet<LogicalData>> sharedDiskToSharedFiles = new TreeMap<String, HashSet<LogicalData>>();

	private static final Logger logger = Logger.getLogger(Loggers.COMM);

	/*
	 * Constructors
	 */
	public LogicalData(String name) {
		this.name = name;
		this.value = null;
		this.onFile = false;
	}

	/*
	 * Getters
	 */
	// No need to sync because it cannot be modified
	public String getName() {
		return this.name;
	}

	public synchronized HashSet<Resource> getAllHosts() {
		HashSet<Resource> list = new HashSet<Resource>();
		for (DataLocation loc : locations) {
			list.addAll(loc.getHosts());
		}		
		return list;
	}

	// Obtain all the URIs that refer all the files
	public synchronized LinkedList<URI> getURIs() {
		LinkedList<URI> list = new LinkedList<URI>();
		for (DataLocation loc : locations) {
			list.addAll(loc.getURIs());
		}
		return list;
	}

	// Obtain one URI per file copy (files in shared disks will only return one URI)
	public LinkedList<URI> getRepresentativeURIs() {
		return this.getURIs();
	}

	public synchronized boolean isInMemory() {
			return this.value != null;
	}

	public boolean isOnFile() {
		return this.onFile;
	}

	public synchronized Object getValue() {
		return this.value;
	}

	public boolean isBeingSaved() {
		return this.isBeingSaved;
	}

	/*
	 * Setters
	 */
	public void addLocation(DataLocation loc) {
		addLocation_private(loc);
	}

	public synchronized void addLocationAndValue(DataLocation loc, Object value) {
		addLocation_private(loc);
		this.value = value;
	}

	public synchronized Object removeValue() {
		DataLocation location = DataLocation.getLocation(appHost, name);
		Object val = this.value;
		this.value = null;
		this.locations.remove(location);

		return val;
	}

	public synchronized void setValue(Object o) {
		this.value = o;
	}

	public synchronized void writeToFile() throws Exception {
		String path = Comm.appHost.getWorkingDirectory() + name;
		DataLocation loc = DataLocation.getLocation(Comm.appHost, path);

		Serializer.serialize(value, path);
		addLocation_private(loc);
	}

	public synchronized void writeToFileAndRemoveValue() throws Exception {
		String path = Comm.appHost.getWorkingDirectory() + name;
		DataLocation new_loc = DataLocation.getLocation(Comm.appHost, path);
		DataLocation old_loc = DataLocation.getLocation(Comm.appHost, name);

		Serializer.serialize(value, path);

		this.locations.remove(old_loc);
		addLocation_private(new_loc);
		this.value = null;
	}

	public synchronized DataLocation removeHostAndCheckLocationToSave(Resource host, HashMap<String, String> sharedMountPoints) {
		lockHostRemove_private();
		DataLocation hostLocation = null;
		boolean hasToSave = true;
		if (isBeingSaved) {
			releaseHostRemoveLock_private();
			return null;
		}

		Iterator<DataLocation> it = locations.iterator();
		while (it.hasNext()) {
			DataLocation loc = it.next();
			if (loc.getType() == Type.PRIVATE) {
				if (loc.getURIInHost(host) != null) {
					hostLocation = loc;
					it.remove();
				} else {
					releaseHostRemoveLock_private();
					return null;
				}
			} else { // is a SharedLocation
				if (loc.getHosts().isEmpty()) {
					String sharedDisk;
					String mountPoint;
					if ((sharedDisk = loc.getSharedDisk()) != null) {
						if ((mountPoint = sharedMountPoints.get(sharedDisk)) != null) {
							hostLocation = DataLocation.getPrivateLocation(
									host, mountPoint + loc.getPath());
						} else {
							releaseHostRemoveLock_private();
							return null;
						}
					} else {
						releaseHostRemoveLock_private();
						return null;
					}
				} else {
					releaseHostRemoveLock_private();
					return null;
				}
			}
		}

		if (hasToSave) {
			this.isBeingSaved = true;
			releaseHostRemoveLock_private();
			return hostLocation;
		} else {
			releaseHostRemoveLock_private();
			return null;
		}
	}

	public Collection<Copy> getCopiesInProgress() {
		LinkedList<CopyInProgress> stored = null;
		boolean done = false;
		while (!done) {
			try {
				stored = inProgress.get(this.name);
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("getCopiesInProgress concurrent modification. Re-calculating");
			}
		}

		if (stored == null) {
			return null;
		}
		LinkedList<Copy> copies = new LinkedList<Copy>();
		for (CopyInProgress cp : stored) {
			copies.add(cp.getCopy());
		}

		return copies;
	}

	public synchronized URI alreadyAvailable(Resource targetHost) {
		for (DataLocation loc : locations) {
			URI u = loc.getURIInHost(targetHost);
			if (u != null) {
				return u;
			}
		}
		return null;
	}

	public Copy alreadyCopying(DataLocation target) {
		LinkedList<CopyInProgress> copying = null;
		boolean done = false;
		while (!done) {
			try {
				copying = inProgress.get(this.name);
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("alreadyCopying concurrent modification. Re-calculating");
			}
		}

		if (copying != null) {
			for (CopyInProgress cip : copying) {
				if (cip.hasTarget(target)) {
					return cip.getCopy();
				}
			}
		}
		return null;
	}

	public void startCopy(Copy c, DataLocation target) {
		LinkedList<CopyInProgress> cips = null;
		
		boolean done = false;
		while (!done) {
			try {
				cips = inProgress.get(this.name);
				if (cips == null) {
					cips = new LinkedList<CopyInProgress>();
					inProgress.put(this.name, cips);
				}
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("startCopy concurrent modification. Re-calculating");
			}
		}

		cips.add(new CopyInProgress(c, target));
	}

	public DataLocation finishedCopy(Copy c) {
		DataLocation loc = null;
		LinkedList<CopyInProgress> cips = null;
		
		boolean done = false;
		while (!done) {
			try {
				cips = inProgress.get(this.name);
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("finishedCopy concurrent modification. Re-calculating");
			}
		}
		
		Iterator<CopyInProgress> it = cips.iterator();
		while (it.hasNext()) {
			CopyInProgress cip = it.next();
			if (cip.c == c) {
				it.remove();
				loc = cip.loc;
				break;
			}
		}

		if (cips.isEmpty()) {
			done = false;
			while (!done) {
				try {
					inProgress.remove(this.name);
					done = true;
				} catch (ConcurrentModificationException cme) {
					logger.debug("finishedCopy concurrent modification. Re-calculating");
				}
			}
		}

		return loc;
	}

	public static HashSet<LogicalData> getAllDataFromHost(Resource host) {
		boolean done = false;
		LinkedList<String> shareds = null;
		while (!done) {
			try {
				shareds = SharedDiskManager.getAllSharedNames(host);
				if (shareds.isEmpty()) {
					if (hostToPrivateFiles.get(host) != null) {
						return hostToPrivateFiles.get(host);
					} else {
						return new HashSet<LogicalData>();
					}
				}
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("getAllDataFromHost concurrent modification. Re-calculating");
			}
		}
		
		HashSet<LogicalData> data = new HashSet<LogicalData>();
		done = false;
		while (!done) {
			try {
				for (String shared : shareds) {
					HashSet<LogicalData> sharedData = sharedDiskToSharedFiles.get(shared);
					if (sharedData != null) {
						data.addAll(sharedData);
					}
				}
				if (hostToPrivateFiles.get(host) != null) {
					data.addAll(hostToPrivateFiles.get(host));
				}
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("getAllDataFromHost concurrent modification. Re-calculating");
				data.clear();
			}
		}
		return data;
	}

	public synchronized void notifyToInProgressCopiesEnd(SafeCopyListener listener) {
		boolean done = false;
		LinkedList<CopyInProgress> copies = null;
		while (!done) {
			try {
				copies = inProgress.get(this.name);
				done = true;
			} catch (ConcurrentModificationException cme) {
				logger.debug("notifyToInProgressCopiesEnd concurrent modification. Re-calculating");
			}
		}

		if (copies != null) {
			for (CopyInProgress copy : copies) {
				listener.addOperation();
				copy.c.addEventListener(listener);
			}
		}
	}

	public synchronized void isObsolete() {
		for (Resource res : this.getAllHosts()) {
			res.addObsolete(name);
		}
	}

	public synchronized void loadFromFile() throws Exception {
		if (value != null) {
			return;
		}

		for (DataLocation loc : locations) {
			URI u = loc.getURIInHost(Comm.appHost);
			if (u == null) {
				continue;
			}
			String path = u.getPath();
			if (path.startsWith(File.separator)) {
				value = Serializer.deserialize(path);
				DataLocation tgtLoc = DataLocation.getLocation(Comm.appHost, name);
				addLocation(tgtLoc);
			}
			return;
		}

		if (value == null) {
			throw new Exception("File does not exists in the master");
		}
	}

	public synchronized void lockHostRemove() {
		lockHostRemove_private();
	}

	public synchronized void releaseHostRemoveLock() {
		releaseHostRemoveLock_private();
	}

	public synchronized String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Logical Data name: ").append(this.name).append(":\n");
		sb.append("\t Value: ").append(value).append("\n");
		sb.append("\t Locations:\n");
		synchronized (locations) {
			for (DataLocation dl : locations) {
				sb.append("\t\t * ").append(dl).append("\n");
			}
		}
		return sb.toString();
	}

	/*
	 * PRIVATE HELPER METHODS
	 */
	private synchronized void addLocation_private(DataLocation loc) {
		isBeingSaved = false;
		locations.add(loc);
		if (logger.isDebugEnabled()){
			logger.debug("Adding location for data "+ this.getName() + " (location: "+ loc.getLocationKey()+")");
		}	
		switch (loc.getType()) {
			case PRIVATE:
				for (Resource host : loc.getHosts()) {
					if (host == null) {
						host = Comm.appHost;
					}
					boolean done = false;
					HashSet<LogicalData> files = null;
					while (!done) {
						try {
							files = hostToPrivateFiles.get(host);
							if (files == null) {
								files = new HashSet<LogicalData>();
								hostToPrivateFiles.put(host, files);
							}
							done = true;
						} catch (ConcurrentModificationException cme) {
							logger.debug("notifyToInProgressCopiesEnd concurrent modification. Re-calculating");
						}
					}
					files.add(this);
				}
				if (loc.getPath().startsWith(File.separator)) {
					onFile = true;
				}
				break;
				
			case SHARED:
				String shared = loc.getSharedDisk();
				boolean done = false;
				HashSet<LogicalData> files = null;
				while (!done) {
					try {
						files = sharedDiskToSharedFiles.get(shared);
						if (files == null) {
							files = new HashSet<LogicalData>();
							sharedDiskToSharedFiles.put(shared, files);
						}
						done = true;
					} catch (ConcurrentModificationException cme) {
						logger.debug("notifyToInProgressCopiesEnd concurrent modification. Re-calculating");
					}
				}
				files.add(this);
				onFile = true;
				break;
		}
	}

	private void lockHostRemove_private() {
		try {
			removeHostLock.acquire();
		} catch (InterruptedException e) {
			logger.error("Exception", e);
		}
	}

	private void releaseHostRemoveLock_private() {
		removeHostLock.release();
	}

	
	private static class CopyInProgress {

		private final Copy c;
		private final DataLocation loc;

		CopyInProgress(Copy c, DataLocation loc) {
			this.c = c;
			this.loc = loc;
		}

		public Copy getCopy() {
			return this.c;
		}

		private boolean hasTarget(DataLocation target) {
			return loc.isTarget(target);
		}

		public String toString() {
			return c.getName() + " to " + loc.toString();
		}

	}

}
