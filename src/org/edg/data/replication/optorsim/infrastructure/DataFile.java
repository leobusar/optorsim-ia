package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Date;
import java.util.LinkedList;
/**
 * Class DataFile is used to create file objects which contain information on the
 * filename, size, pinning status, master status and creation time of the files.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4 
 */
public class DataFile {

    private String _lfn;
    private int _size;
    private int _fileIndex;
    private boolean _master;
    private Date _timestamp;
    private int _pin;
    private LinkedList _accessList = new LinkedList();
    private StorageElement _se;

    private double _lastEstimatedValue = 0;
    private GridTime _time = GridTimeFactory.getGridTime();

    /**
     * Construct a DataFile object with the given parameters. 
     * @param lfn the Logical File Name of this DataFile
     * @param size the size in MB of this DataFile
     * @param fileIndex a unique integer identifier for this file
     * @param pin true if this file is pinned, false otherwise
     * @param master true if this is the master copy of this lfn, false otherwise
     */
    public DataFile(String lfn,
			int size,
			int fileIndex,
			boolean pin,
			boolean master) {

		_lfn = new String(lfn);
		_master = master;
		_size = size;
		_fileIndex = fileIndex;
		_timestamp = _time.getDate();
		_se = null;
		
			// The pin counter should be 1 if we want this file pinned.
		_pin = pin ? 1 : 0;
	}

    
    /**
     * Get a file's Storage Element
     * @return the SE the file is stored on, or null if it isn't stored on any SE
     */
    public StorageElement se() {
    	return _se;
    }
    
    /**
     * A meaningful string describing this file.
     */
    public String toString() {
    	String seName = (_se != null) ? _se.toString() : "(lost in space)";
    	return "{"+_lfn+" on " + seName+"}";
    }
    
    /**
     * Inform a DataFile it is now stored on a particular StorageElement
     * @param se the storage element the file is now stored on.
     */
    public void registerSE( StorageElement se) {
    	_se = se;
    }
    
    /**
     * Dissociate this file from any SE.
     *
     */
    public void deregisterSE(){
    	_se = null;
    }


    /**
     * Get the logical file name of this file.
     * @return The name of the file
     */
    public String lfn() {
		return _lfn;
    }
	
    /**
     * Get the size of this file in MB.
     * @return The size of the file
     */
    public int size() {
		return _size;
    }

    /**
     * Get the index of this file.
     * @return The file index
     */
    public int fileIndex() {
		return _fileIndex;
    }


    /**
     * Get the last estimated value of this file.
     * @return The last estimated value of the file.
     */
    public double lastEstimatedValue() {
		return _lastEstimatedValue;
    }

    /**
     * Set the last estimated value of the file.
     */
    public void setLastEstimatedValue(double value) {
		_lastEstimatedValue = value;
    }

    /**
     * Add a pin to the file. The file cannot be removed unless the
     * number of pins on it is zero. 
     */
    public synchronized void addPin() {
		_pin++;
       }

    /**
     * Release a pin on the file.
     */
    public synchronized void releasePin() {
    	if( _pin == 0 ) {
    		System.out.println( "Trying to remove a non-existent pin from "+this+".");
    		return;
    	}
		_pin--;
		}

    /**
     * Get the pin status of this file.
     * @return The pin status.
     */
    public synchronized boolean isPinned() {
		return _pin>0;
    }


    /**
     * Check whether this file is deleteable.
     * @return True if the file is not a master file
     * and is not pinned, false otherwise.
     */
    public synchronized boolean isDeleteable() {
		return (_pin == 0 && !_master && _se != null);
    }

    /**
     * Get the time at which this file was created.
     * @return the timestamp of the file
     */
    public long createTime() {
		return _timestamp.getTime();
    }


    /**
     * Get the time at which this file was last accessed.
     * @return The time of the last access if there are
     * any, the timestamp otherwise.
     */
    // TODO: We should get this information from the access history,
    // not the files themselves.
    public long getLastAccessTime() {
		if( _accessList.size() > 0 ) {
	    	return ((Date)_accessList.getLast()).getTime();
		}
		return _timestamp.getTime();
    }

    /**
     * Called whenever the file is accessed to update the access history.
     */
    // TODO: get rid of this method. Access history should go in SE.
    public void read() {
		_accessList.add(_time.getDate());
    }

    /**
     * To get round the fact that Object.clone() is protected.
     * @return A new DataFile with identical lfn, size and file
     * index as this one.
     */
    public DataFile cloneFile() {
    	DataFile cloneFile = new DataFile( 
							_lfn,
						  _size,
						  _fileIndex,
						  true,
						  false);
		cloneFile.registerSE( _se);
		return cloneFile;    	
    }

}
