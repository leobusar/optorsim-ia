package org.edg.data.replication.optorsim.optor;

import java.util.*;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.Statistics;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;

/**
 * The simplest implementation of StorageElement, providing all
 * the basic file handling methods and a simple implementation
 * of fileToDelete(). All other StorageElements
 * extend this class and override fileToDelete()
 * with a method dependent on the optimisation algorithm.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class SimpleStorageElement implements StorageElement {

	private GridSite _site;
	private int _seId;
	private String _seName;
	private long _capacity;
	private long _volumeFilled = 0;

	//	Maps file name to DataFile object
	private Hashtable _storage;
	
    /**
     * Create a new SimpleStorageElement on the given GridSite
     * with the given capacity.
     */
	public SimpleStorageElement( GridSite site, long capacity) {
		_site = site;			
		_capacity = capacity;
		_storage = new Hashtable();
		_seId = site.registerSE(this);
		_seName = "SE"+_seId+"@"+_site;
	}

	/**
	* Method to check if SE is full.
	* @return true if SE is full, false otherwise.
	*/
	public boolean isFull() {
		if( _volumeFilled == _capacity)
			return true;
		return false;
	}

	/**
	 * Enquire of a StorageElement on which GridSite it is located
	 * @return The GridSite of the SE
	 */
	public GridSite getGridSite() {
		return _site;
	}

	/**
	 * Returns a String representing the SimpleStorageElement's
	 * name, in the form "SE[id]@Site[site number]".
	* @return The name of this SE.
	*/
	public String toString() {
		return _seName;
	}
	
	/**
	 * Returns the available space on the SE.
	* @return the available space on the SE.
	*/
	public synchronized long getAvailableSpace() {
		return _capacity - _volumeFilled;
	}

	/**
	 * Returns the total capacity of the SE.
	* @return the total capacity of the SE.
	*/
	public synchronized long getCapacity() {
		return _capacity;
	}

	/**
	 * Returns the statistics of the SE.
	 * @return the statistics of the SE.
	*/
	public Statistics getStatistics() {
		Map stats = new HashMap();
		stats.put((String)"usage", new Float(_volumeFilled));
		stats.put((String)"capacity", new Float(_capacity));
		return new Statistics(this, stats);
	}

	/**
	 * Returns all the files on the SE in the form of a Hashtable.
	 * @return all the files on the SE.
	 */
	public synchronized Hashtable getAllFiles() {
		return _storage;
	}

	/**
	* Returns the number of files on the SE.
	* @return The number of files on the SE.
	*/
	public int numberOfStoredFiles() {
		return _storage.size();
	}

	/**
	 * Method used at the start of a file transfer to ensure the space
	 * reserved for the replicated file is not used. When the file
	 * transfer has completed, {@link #addPreReservedFile} should be
	 * called to register the file with the StorageElement.
	 * @return true if operation succeeded
	*/
	public synchronized boolean reserveSpace( int filesize) {
		if( _volumeFilled + filesize > _capacity)
			return false;
		_volumeFilled += filesize;
		return true;
	}

	/**
	 * Add a DataFile to the SE. This method should only
	 * be used for files added "instantaneously" to the SE, for
	 * example when files are distributed around the Grid at the
	 * start of the simulation. For file replication 
	 * {@link #reserveSpace} followed by {@link #addPreReservedFile}
	 * should be used.
	 * @return true if successful, false if not.
	*/
	public synchronized boolean addFile(DataFile file) {		
		if( hasFile( file.lfn()))
			return false;

		if( _volumeFilled + file.size() > _capacity)
			return false;
						
		_volumeFilled += file.size();
		file.registerSE( this);
		_storage.put(file.lfn(), file);
		return true;
	}

	/**
	 * Query the StorageElement, asking it to list all stored files
	 * in a human readable format.
	 * @return A textual description of all stored files
	*/
	public String listFiles() {
		String fileList = new String();
		
		boolean firstEntry = true;

		for (Enumeration eFile = _storage.keys();
				eFile.hasMoreElements();) {

			String file = (String)eFile.nextElement();
				
			if( !firstEntry)
				fileList = fileList.concat(" ");
			else
				firstEntry = false;
				fileList = fileList.concat( file);
		}
		if( fileList.length() == 0)
			fileList = " no data files";
		return fileList;
	}

	/**
	 * Add a DataFile to the SE. This assumes {@link #reserveSpace}
	 * has been called first and is called at the end of a file
	 * transfer to register the file with the SE.
	*/
	public synchronized void addPreReservedFile(DataFile file) {
		_storage.put(file.lfn(), file);
		file.registerSE( this);
	}

	/**
	* Remove a file from the list (only works if file is not pinned).
	*/
	public synchronized void removeFile(DataFile file) {
		if( file.isPinned()) {
			System.out.println("Warning: Attempting to delete a pinned file "+file );
			return;
		}
			
		_storage.remove(file.lfn());
		_volumeFilled -= file.size();
		file.deregisterSE();
	}

	/**
	 * Method used to query the SE if it has a DataFile whose
	 * name matches the given String.
	 * @return true if the SE has such a file, false if not.
	*/
	public synchronized boolean hasFile( String lfn) {
		if( _storage.containsKey( lfn))
			return true;
		else
			return false;
	}

	/**
	* This method returns the DataFile object whose name is 
	* specified in the method argument. If the file isn't
	* stored on this SE, then null is returned.
	* @return the DataFile object corresponding to filename,
	* or null if no replica is present.
	*/
	public synchronized DataFile requestFile(String filename) {
		return (DataFile)_storage.get(filename);
	}

    /**
     * Does nothing here since we don't keep an access history
     * in this SE.
     */
    public void accessFile(DataFile file) {
    }

	/**
	* This method returns a DataFile selected randomly from
	* all the DataFiles on the SE for deletion.
	* @return The file to be deleted.
	*/
	public List filesToDelete(DataFile file) {
				
		ArrayList files = new ArrayList(_storage.values());
		ArrayList filesToDelete = new ArrayList();
		int totalSize = 0;
		do {
			DataFile df = (DataFile)files.get((int)(Math.random()*files.size()));
			if(df.isDeleteable()) {
				filesToDelete.add(df);
				files.remove(df);
				totalSize += df.size();
			}
		}
		while(totalSize < file.size());
		
		return filesToDelete;
	}

    /**
     * @see org.edg.data.replication.optorsim.infrastructure.StorageElement#isTherePotentialAvailableSpace(DataFile newFile)
     */
    public synchronized boolean isTherePotentialAvailableSpace(DataFile newFile) {
    	
    	long potentialSpace = getAvailableSpace();
		if(potentialSpace >= newFile.size()) return true;
		
        for(Enumeration e = _storage.elements(); e.hasMoreElements();){
        	DataFile file = (DataFile) e.nextElement();
        	if(file.isDeleteable()) potentialSpace += file.size();
        	if(potentialSpace >= newFile.size()) return true;
        }
        return false;
    }

}
