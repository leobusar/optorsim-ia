package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;

/**
 * Used to print debugging information during the auction process.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
abstract public class Debugger {

	private static File _logFile;
	private static PrintStream _out;
	private static OptorSimParameters _params;
    private static GridTime _time = GridTimeFactory.getGridTime();

    /**
     * Set up the auction log file.
     */
    public static void initialise() {
        _params = OptorSimParameters.getInstance();

        if(_params.useAuctionDebugger()){
            try {
                _logFile = new File("auction.log");
                _logFile.delete();
                _logFile.createNewFile();
                _out =	new PrintStream(new FileOutputStream(_logFile, true));

                _out.println(" *** Auction log began "+(new Date()).toString()
                        +" ***\n");
            }
            catch(Exception e) {
            }
        }
    }

    /**
     * Print string to the auction log file.
     * @param message The string to print to the file.
     */
    public static synchronized void printDebugMessage(String message) {
		if(_params.useAuctionDebugger())
	    	_out.println(_time.getTimeMillis()+" "+message);
    }

    /**
     * Print information on the bids for a particular auction.
     */
    public static synchronized void printBids(String mediatorName,
					      int auctionId,
					      int nestingLevel,
					      List bids) {

		_out.println("P2P "+ mediatorName +"> Auction "+ auctionId +
		 	   "/" + nestingLevel  + ": collected "+ bids.size()+ 
		    	" offers from local SBs; Bids: ");	    
		for(int i=0; i<bids.size(); i++) {
	    	_out.println(" "+((Bid)bids.get(i)).getOffer() );
		}
		_out.println("\n");
		_out.flush();
    }

}

