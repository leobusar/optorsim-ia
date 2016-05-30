package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.OptorSimGUI;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/**
 * FileTransfer takes care of file transfers over the simulated
 * Grid. It notifies other FileTransfers that it is starting (and
 * therefore changing the network situation) and waits until the
 * transfer is complete. If the wait is interrupted by another
 * FileTransfer starting, the time left to transfer the file is
 * recalculated based on the new network load and then it tries
 * to wait again until the transfer is complete. When the transfer
 * has finished it notifies all the other currently running 
 * FileTransfers.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 *
 * @since JDK1.4
 */
public class FileTransfer {
	
	/*** The number of bits in a byte (8)*/
    final static int BITS_IN_BYTE=8;
    /** The number of ms in a second (1000)**/
    final static int MILLISECONDS_IN_SECOND=1000;
    /** The number of ms in half an hour (1800000).**/
    final static int HALF_AN_HOUR=1800000;
	
	private	GridTime _time = GridTimeFactory.getGridTime();
	
    /**
     * Called by the GridContainer to execute the transfer
     * of a file between two sites along the specified route.
     */
    public void transferFile( GridSite site1, GridSite site2,
			     int fileSize) {
		
		if(OptorSimParameters.getInstance().useGui())
			OptorSimGUI.filesTransferred(site1, site2, fileSize);
		float transferred;
		int unitFactor = BITS_IN_BYTE * MILLISECONDS_IN_SECOND;

		GridContainer gc = GridContainer.getInstance();
		gc.addConnection( site1, site2);

		FileTransferFactory.notifyAllFileTransferStarted(this);

		for( float fileSizeLeft = fileSize; fileSizeLeft > 0; fileSizeLeft -= transferred) {

		    float bw = gc.currentBandwidth( site1, site2);
		    //long startTime = (new Date()).getTimeMillis();
            long startTime = _time.getTimeMillis();
		    long timeToWait = Math.min((long)(fileSizeLeft*unitFactor/bw), HALF_AN_HOUR);

            // sometimes happens due to rounding in calculations
            if (timeToWait == 0) break;
      //      System.out.println(this.toString()+"> transferring file...");
            _time.gtWait(this, timeToWait);
	    	
		    transferred = bw*(_time.getTimeMillis() - startTime)/unitFactor;
		}

		gc.dropConnection( site1, site2);
		FileTransferFactory.notifyAllFileTransferEnded(this);
		if(OptorSimParameters.getInstance().useGui()) 
			OptorSimGUI.fileTransferStopped(site1, site2, fileSize);
   }

    /**
     * Called by the notifyAllFileTransfers() method of
     * FileTransferFactory to wake up this FileTransfer.
     */
    public void notifyFT() {
		_time.gtNotify(this);
    }
    
}
