package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.JobConfFileReader;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.reptorsim.ReplicaManager;
import org.edg.data.replication.optorsim.time.GridTimeFactory;
import org.edg.data.replication.optorsim.auctions.Debugger;

import java.util.Iterator;

/**
 * Sets up the simulation and starts the threads. A configuration
 * file containing an initial static bandwidth matrix and CE/SE status is
 * read.  The routes from the individual sites are found, and then the
 * ComputingElement and Resource Broker threads are started.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class OptorSimMain {

    private OptorSimParameters _params;
    
    public OptorSimMain() {
    }
   
    /**
     * The main method sets up the simulation.
     */
    public static void main(String[] args) {
		OptorSimMain optorSimMainInstance = new OptorSimMain();

		System.out.println( "                 ============= O P T O R S I M =============\n");
		if(args.length!=1 && args.length!=0) {
	    	optorSimMainInstance.usage();
		} 

		// Store the Parameters filename
		if(args.length==1) 
	    	OptorSimParameters.setFilename( args[0]);
		else {
	    	System.out.println("OptorSimMain> using default parameters file examples/parameters.conf");
	    	OptorSimParameters.setFilename("examples/parameters.conf");
		}

		optorSimMainInstance.init();
		optorSimMainInstance.run();
	}

    /**
     * Reads the configuration file and creates hashtable of routes.
     * Initialises SEs and fills them with files, then initialises
     * CEs and starts their threads.
     */
    private void init() {

		_params = OptorSimParameters.getInstance();
			// Initialise networkInfo
		GridConfFileReader gridconffilereader = GridConfFileReader.getInstance();

		// Distribute files between storage elements:
		initStorageElements();


		if(_params.useGui()) {
			new OptorSimGUI();
				// make sure RB is paused initially.  The GUI will start if off.
			ResourceBrokerFactory.getInstance().pauseRB();
		}
    }



    /**
     * A method to start all P2PMediators, Computing Element  and Resource Broker threads
     */
    private void run() {
		GridContainer gc = GridContainer.getInstance();

		ThreadGroup optorThreads = new ThreadGroup("optor");
        GridTimeFactory.setThreadGroup(optorThreads);

        if( _params.auctionOn()) {
            Debugger.initialise();
			gc.startAllP2P();
        }

		gc.startAllCEs();

		// Start ResourceBroker going.
		Thread rbThread = new Thread( optorThreads, ResourceBrokerFactory.getInstance());
		rbThread.start();

        // start the users submitting jobs
        Thread usersThread = new Thread( optorThreads, UsersFactory.getUsers());
        usersThread.start();

    }

    /**
     * Distributes the files to the SEs.
     */
    private void initStorageElements() {
		JobConfFileReader jread = JobConfFileReader.getInstance();
		Iterator iFiles = jread.assignFilesToSites();

		ReplicaManager rm = ReplicaManager.getInstance();
		
		while( iFiles.hasNext()) {
			DataFile file = (DataFile) iFiles.next();
			rm.registerEntry( file);
		}
   	}


    /**
     * Prints correct arguments to use with run command.
     */
    private void usage() {
		System.out.println("\n ***  Help in using OptorSim ***\n");
		System.out.println(" Use the appropriate script for your operating system:");
		System.out.println();
		System.out.println(" Bourne shells : OptorSim.sh");
		System.out.println("    MS Windows : OptorSim.bat\n");
		System.out.println(" Usage: OptorSim.sh/bat [parameters file]");
		System.out.println(" If no parameters file is specified the default edg-optorsim/examples/parameters.conf is used\n");
		System.out.println(" For further instructions see the OptorSim userguide.\n");
		System.exit(1);
    }
}
