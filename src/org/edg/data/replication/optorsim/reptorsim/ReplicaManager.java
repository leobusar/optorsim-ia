package org.edg.data.replication.optorsim.reptorsim;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;

/**
 * The ReplicaManager provides file manipulation methods and interfaces
 * between the Computing Elements and optimisers and the low level
 * Grid infrastructure. It also acts as a wrapper class for the
 * ReplicaCatalogue, so that any changes to it must go through the
 * methods of this class.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class ReplicaManager {

    /**
     * STATIC singleton handler
     */

    private static ReplicaManager _replicaManagerInstance = null;

    /** Returns the (singleton) instance of the ReplicaManager.*/ 
    public static ReplicaManager getInstance() {
		if( _replicaManagerInstance == null) 
	    	_replicaManagerInstance = new ReplicaManager();

		return _replicaManagerInstance;
    }


    private ReplicaCatalogue _rc;
    private final int ALL_OF_FILE=1;

    private ReplicaManager() {
		_rc = new ReplicaCatalogue();
    }

    /**
     * Register an entry in the ReplicaCatalogue.
     */
    public void registerEntry( DataFile file) {
		_rc.addFile( file);
    }

    /**
     * Remove an entry from the ReplicaCatalogue.
     */
    public void unregisterEntry( DataFile file) {
		_rc.removeFile( file);
    }

    /**
     * Copy a file to a SE, and register it in the Replica Catalogue.
     * @param source The file we wish to replicate
     * @param toSE  The destination SE
     * @return The new replica if successful, null otherwise
     */
    public DataFile replicateFile( DataFile source, StorageElement toSE) {
		GridContainer gc = GridContainer.getInstance();
		DataFile newFile = gc.replicate( source, toSE);

		if( newFile != null)
			_rc.addFile( newFile);
		return newFile;
    }

    /**
     * Copy a file to a SE, but do not register it in the 
     * Replica Catalogue. The file is "cached" in the destination SE.
     * @param source The file we wish to copy
     * @param toSE  The destination SE
     */
    public void copyFile( DataFile source,  StorageElement toSE) {
		GridContainer gc = GridContainer.getInstance();			
		gc.copy( source, toSE.getGridSite(), ALL_OF_FILE);
    }

    /**
     * Delete a DataFile from the Grid, physically deleting it
     * from the Storage Element and unregistering it with the 
     * Replica Catalogue.
     */
    public void deleteFile( DataFile file) {
		_rc.removeFile( file);
	
		StorageElement se = file.se();
		se.removeFile(file);
    }

    /**
     * Returns an array of all the replicas of filename on the Grid.
     * @return Array of DataFiles which are replicas of filename.
     */
    public DataFile[] listReplicas(String filename) {
		return _rc.getDataFilesArray(filename);

    }	

}




