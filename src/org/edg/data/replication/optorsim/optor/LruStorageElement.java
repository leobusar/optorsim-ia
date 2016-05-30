package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

import java.util.*;

/**
 * This StorageElement deletes the file stored on it that was
 * accessed least recently.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class LruStorageElement extends AccessHistoryStorageElement {

    /**
     * @param site The GridSite on which the SE is situated.
     * @param capacity The total capacity of this SE.
     */
    public LruStorageElement(GridSite site, long capacity) {
	super(site, capacity);
    }

    /**
     * Returns the DataFiles that were accessed least recently
     * in the access history. If there are files on the SE
     * not in the access history these are preferentially used
     * since they have the most recent access older than any in
     * the access history.
     * @return A List of least recently accessed DataFiles on the SE.
     */
    public List filesToDelete(DataFile newFile) {

		long dt = OptorSimParameters.getInstance().getDt();	
		List recentHistory = new LinkedList(getRecentAccessHistory( dt).values());
		Stack fileAccessOrder = new Stack();
		List filesToDelete = new LinkedList();
		List nonAHFiles = new LinkedList();
		long deleteableFileSize = getAvailableSpace();
	
		// sort history into a list of deletable DataFiles in order of most recent access
        // Go through the history in reverse order (starting with most recent)
        // and add to the stack the first time a file appears. We can then pop
        // the least recently accessed file off the stack
		for( ListIterator i = recentHistory.listIterator(recentHistory.size());
             i.hasPrevious(); ) {
	
		    DataFile historyFile = (DataFile)i.previous();
		    if (historyFile.isDeleteable() && !fileAccessOrder.contains(historyFile))
	    		fileAccessOrder.push(historyFile);
		}
		
		// first find all the files not on the access history
		for( Enumeration e = getAllFiles().elements(); e.hasMoreElements();) {

			DataFile file = (DataFile)e.nextElement();
			if( file.isDeleteable() && !fileAccessOrder.contains(file)) {
				nonAHFiles.add(file);
			}
		}
        
		// pick random files until we have enough space or the list is empty
		while (!nonAHFiles.isEmpty()) {
			DataFile file = (DataFile)nonAHFiles.get((int)(Math.random()*nonAHFiles.size()));
			filesToDelete.add(file);
			nonAHFiles.remove(file);
			deleteableFileSize += file.size();
			if (deleteableFileSize >= newFile.size()) return filesToDelete;
		}

		// if this didn't yield enough space take files from the list
		do {
			DataFile deadFile;
            try {
                deadFile = (DataFile) fileAccessOrder.pop();
            } catch (EmptyStackException e) {
                System.out.println("Warning: couldn't delete enough files to replicate " +
                	newFile+" when it should have been possible. Have to use remote i/o");
                return null;
            }
			filesToDelete.add(deadFile);
			deleteableFileSize += deadFile.size();
		}
		while (deleteableFileSize < newFile.size() );		
	
		return filesToDelete;
    }

}
