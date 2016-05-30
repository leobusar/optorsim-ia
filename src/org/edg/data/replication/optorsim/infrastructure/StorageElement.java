package org.edg.data.replication.optorsim.infrastructure;

import java.util.Hashtable;
import java.util.List;

/** 
 * The StorageElement (SE) is where DataFiles in the Grid are
 * stored. It represents a hard disk with methods which enable
 * the Grid to manipulate the data stored on it. This interface
 * describes the basic file handling methods such as adding files,
 * deleting files and querying how much space is available. The
 * most interesting method in terms of optimisation is
 * {@link #filesToDelete}. Each implementation defines its own algorithm
 * to control which file(s) should be deleted if the storage is full
 * and space must be made for a new file. This algorithm defines
 * the optimisation strategy being used.
 * <p>
 * SimpleStorageElement provides the basic implementation
 * of this interface and all other StorageElements extend 
 * SimpleStorageElement and overload {@link #filesToDelete} 
 * with their own optimisation algorithm.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
*/
public interface StorageElement {

	/**
	 * Check whether SE is full.
	 * @return true if SE is full, false otherwise
	 */
	boolean isFull();

	/**
	 * Enquire of a StorageElement on which GridSite it is located
	 * @return The GridSite of the SE
	 */
	GridSite getGridSite();

	/**
	 * Returns the name of the SE.
	 * @return The name of the SE.
	 */
	String toString();

	/**
	 * Returns the available space on the SE.
	 * @return the available space on the SE.
	 */
	long getAvailableSpace();

	/**
	 * Returns the total capacity of the SE.
	 * @return the total capacity of the SE.
	 */
	long getCapacity();

	/**
	 * Calculates whether there space could be created for a new files
	 * by deleting files that are not masters or pinned.
	 * @return Whether potential space can be made available for a new file.
	 */
	boolean isTherePotentialAvailableSpace(DataFile newFile);

	/**
	 * Returns the statistics of the SE.
	 * @return the statistics of the SE.
	 */
	Statistics getStatistics();

	/**
	 * Returns all the files on the SE in the form of a Hashtable.
	 * @return all the files on the SE.
	 */
	Hashtable getAllFiles();

	/**
	* Returns the number of files on the SE.
	* @return The number of files on the SE.
	*/
	int numberOfStoredFiles();

	/**
	 * Method used at the start of a file transfer to ensure the space
	 * reserved for the replicated file is not used. When the file
	 * transfer has completed, {@link #addPreReservedFile} should be
	 * called to register the file with the StorageElement.
	 * @return true if operation succeeded
	 */
	boolean reserveSpace(int filesize);

	/**
	 * Query the StorageElement, asking it to list all stored files
	 * in a human readable format.
	 * @return A textual description of all stored files
	 */
	String listFiles();

	/**
	 * Add a DataFile to the SE. This method should only
	 * be used for files added "instantaneously" to the SE, for
	 * example when files are distributed around the Grid at the
	 * start of the simulation. For file replication 
	 * {@link #reserveSpace} followed by {@link #addPreReservedFile}
	 * should be used.
	 * @return true if successful, false if not.
	 */
	boolean addFile(DataFile file);
	
	/**
	 * Add a DataFile to the SE. This assumes {@link #reserveSpace}
	 * has been called first and is called at the end of a file
	 * transfer to register the file with the SE.
	 */
	void addPreReservedFile(DataFile file);
	
	/**
	 * Remove a file from the SE (only works if file is not pinned).
	 */
	void removeFile(DataFile file);

	/**
	 * Method used to query the SE if it has a DataFile whose
	 * name matches the given String.
	 * @return true if the SE has such a file, false if not.
	 */
	boolean hasFile( String name);
	
	/**
	 * This method returns the DataFile object whose name is 
	 * specified in the method argument.
	 * @return The DataFile corresponding to filename.
	 */
	DataFile requestFile(String filename);

    /**
     * register an access to a DataFile on this SE.	
     */
    void accessFile(DataFile file);

	/**
	 * If the SE is full and a new file is to be replicated to it
	 * this method is called to choose a file to delete to create
	 * the space required. Each StorageElement's implementation
	 * of this method defines the optimisation algorithm used.
	 * @return The List of files to be deleted when space is needed on the SE.
	 */
	 List filesToDelete(DataFile file);
	
}
