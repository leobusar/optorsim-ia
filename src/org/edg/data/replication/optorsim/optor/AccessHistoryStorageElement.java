package org.edg.data.replication.optorsim.optor;

import java.util.Date;
import java.util.TreeMap;
import java.util.Iterator;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/**
 * An extension of SimpleStorageElement which keeps a record of 
 * file accesses. This is implemented as a {@link TreeMap} mapping
 * a {@link Date} to a {@link DataFile}. When a file is accessed on
 * the SE a new entry is added to the access history. Optimisation
 * algorithms in StorageElement classes that extend this class use
 * the recent access history to evaluate the relative values of
 * files stored on the SE.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class AccessHistoryStorageElement extends SimpleStorageElement {

	private TreeMap _accessHistory = new TreeMap();
    private GridTime _time = GridTimeFactory.getGridTime();

	/**
	 * Construct an AccessHistoryStorageElement object on GridSite <i> site </i>
	 * with storage capacity <i> capacity </i>.
	 * @param site The GridSite on which the SE is situated.
	 * @param capacity The total capacity of this SE.
	 */
	public AccessHistoryStorageElement(GridSite site, long capacity) {
		super(site, capacity);		
	}

	/**
	* Return the most recent section of the access history, 
	* covering the <b>dt</b> milliseconds up to the last entry.
	*/
	public synchronized TreeMap getRecentAccessHistory( long dt) {
		// if access history is empty just return the empty TreeMap
		if (_accessHistory.isEmpty()) return new TreeMap(_accessHistory);

        TreeMap ref = null;
        try {
            Date lastEntryTime = (Date)_accessHistory.lastKey();
            Date timeLimit = new Date( lastEntryTime.getTime() - dt );
            ref = new TreeMap(_accessHistory.tailMap(timeLimit));

            // We overwrite the old data, hopefully the garbage collector
            // will do its thing
            _accessHistory = new TreeMap(ref);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return new TreeMap(_accessHistory);
        }
        return ref;
	}
	
	/**
	* Return the most recent section of the access history,  
	* but mapped to the file indices rather than the files themselves.
	*/
	public synchronized TreeMap getRecentAccessHistoryIndices( long dt) {

	    // TODO: we shouldn't need this method. This information can 
	    // already be got from the above method.
	    TreeMap ref = getRecentAccessHistory(dt);
	    TreeMap indexHistory = new TreeMap();

	    for( Iterator i = ref.keySet().iterator(); i.hasNext(); ) {
		Date timeStamp = (Date)i.next();
		int fileIndex = ((DataFile)ref.get(timeStamp)).fileIndex();
		indexHistory.put(timeStamp, new Integer(fileIndex));
	    }

	    return indexHistory;
	}
	
    /**
     * Updated the access history. We should only need one method
     * for this once all access history has been taken out of DataFile.
     */
    public synchronized void accessFile(DataFile file) {
        _accessHistory.put(_time.getDate(),  file);
        // TODO: get all file history out of DataFile.
        file.read();
    }
}
