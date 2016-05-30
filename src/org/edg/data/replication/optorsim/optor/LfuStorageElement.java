package org.edg.data.replication.optorsim.optor;

import java.util.*;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * A StorageElement where the file to delete is chosen using a Least 
 * Frequently Used (LFU) algorithm.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class LfuStorageElement extends AccessHistoryStorageElement {

    /**
     * @param site The GridSite on which the SE is situated.
     * @param capacity The total capacity of this SE.
     */
    public LfuStorageElement(GridSite site, long capacity) {
		super(site, capacity);
    }

    /**
     * Returns the DataFiles which have been accessed least
     * frequently in recent access history. If there is a file
     * on the SE which is not in the access history this is
     * used first since it has been accessed the least number of
     * times (ie zero).
     * @return The least frequently accessed files on the SE.
     */
    public List filesToDelete(DataFile newFile) {	
		
		DataFile chosenFile = null;
		long dt = OptorSimParameters.getInstance().getDt();	
		Map recentHistory = getRecentAccessHistory( dt);
		Map fileCount = new HashMap();
		List filesToDelete = new LinkedList();
		List nonAHFiles = new LinkedList();
		long deleteableFileSize = getAvailableSpace();
	
		for( Iterator i = recentHistory.values().iterator(); i.hasNext(); ) {
	
		    // sort history into a map of filenames to their no. of accesses
		    String historyFile = ((DataFile)i.next()).lfn();
	
		    if( fileCount.containsKey(historyFile)) {
				int count = ((Integer)fileCount.get(historyFile)).intValue();
				fileCount.put(historyFile, new Integer(++count));
		    }	
		    else {
				fileCount.put(historyFile, new Integer(1));
		    }
		}
	
		// first find all the files not on the access history
        for( Enumeration e = getAllFiles().elements(); e.hasMoreElements();) {

            DataFile file = (DataFile)e.nextElement();
            if( file.isDeleteable() && !fileCount.containsKey(file.lfn())) {
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
            int minCount = Integer.MAX_VALUE;
			for( Iterator i = fileCount.keySet().iterator(); i.hasNext();) {
		
				DataFile file = requestFile((String)i.next());
				
				if( file != null && file.isDeleteable() ) {
					int accesses = ((Integer)fileCount.get(file.lfn())).intValue();
					if(accesses < minCount) {
		
				    	chosenFile = file;
				    	minCount = accesses;
					}
			    }
			}
			filesToDelete.add(chosenFile);
			deleteableFileSize += chosenFile.size();

            if (fileCount.remove(chosenFile.lfn()) == null) {
                // this means there were no deleteable files left so perhaps
                // one was pinned during the operation
                System.out.println("Warning: couldn't delete enough files to replicate " +
                      newFile+" when it should have been possible. Have to use remote i/o");
                return null;
            }
		}
		while (deleteableFileSize < newFile.size() );		
	
		return filesToDelete;
    }

}
