package org.edg.data.replication.optorsim.reptorsim;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import java.util.Hashtable;
import java.util.Set;
import java.util.HashSet;

/**
 * This class is a catalogue of files available on the Grid. Logical
 * File Names (strings representing a unique file ) are mapped
 * to the physical DataFiles (individual instances or replicas of each
 * file) spread around the Grid using a Hashtable. The keys are the 
 * Logical File Names (string) and the values are Sets of DataFiles.
 * The ReplicaCatalogue is not used directly but is accessed via the
 * ReplicaManager which provides methods to for example copy a file
 * around the Grid or remove a file from the Grid.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */ 
class ReplicaCatalogue {
	private Hashtable _catalogue;
	
	protected ReplicaCatalogue() {
		_catalogue=new Hashtable();
	}

    /**
     * Add a new DataFile to the catalogue.
     */
	protected synchronized void addFile( DataFile file) {
		String logicalFileName = file.lfn();
		
		if( isStored( file)) {
			System.out.println("RC> ERROR: File "+file+" already registered");
			return; 
		}

		Set fileCollection = getDataFiles( logicalFileName);
		
		if( fileCollection == null) {
			fileCollection = new HashSet();
			_catalogue.put( logicalFileName, fileCollection);
		}
			
		fileCollection.add( file);
	}

	/**
	 * Check to see if a particular DataFile is registered.
	 * @param file The file to check
	 * @return whether the file is registered
	 */	
	protected synchronized boolean isStored( DataFile file) {
		String lfn = file.lfn();
		
		if( ! _catalogue.containsKey(lfn))
			return false;
			
		Set replicas = getDataFiles( lfn);
		return replicas.contains( file);
	}

    /**
     * Return the Set of Datafiles associated with the Logical File Name
     * logicalFileName, ie all the replicas of the file named
     * logicalFileName.
     */
	protected synchronized Set getDataFiles(String logicalFileName) {
		return (Set)_catalogue.get(logicalFileName);
	}

    /**
     * Return the Set of Datafiles associated with the Logical File Name
     * logicalFileName as an array.
     */
	protected synchronized DataFile[] getDataFilesArray(String logicalFileName) {
		Set replicaSet = getDataFiles(logicalFileName);

		if( replicaSet == null)
		    return new DataFile[0];

		DataFile replicaArray[] = new DataFile[replicaSet.size()];
		replicaSet.toArray( replicaArray);

		return replicaArray;
	}

    /**
     * Remove a DataFile from the catalog.
     */
	protected synchronized void removeFile( DataFile file) {
		String logicalFileName = file.lfn();
		Set deletedFile = getDataFiles( logicalFileName);
		
		if (deletedFile.contains(file))
			deletedFile.remove(file);
		else
			System.out.println("RC> ERROR: Cannot delete a file that does not exist!");
	}

}
