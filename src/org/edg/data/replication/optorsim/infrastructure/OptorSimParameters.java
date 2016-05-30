package org.edg.data.replication.optorsim.infrastructure;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * This class holds information on the various parameters
 * that can be varied in the simulation. These are read in from
 * a file specified on the command line.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class OptorSimParameters {

    private Properties _table;
    private static OptorSimParameters _optorSimParametersInstance = null;
    private static String _parametersFilename = null;

    /**
     * Returns the single instance of the OptorSimParameters object,
     * creating it if it has not already been instantiated.
     * @return The OptorSimParameters instance.
     */
    public static OptorSimParameters getInstance() {
	if( _optorSimParametersInstance == null)
	    _optorSimParametersInstance = new OptorSimParameters();

	return _optorSimParametersInstance;
    }

    /**
     * Sets the name of the OptorSim parameters file. This is called
     * by {@link org.edg.data.replication.optorsim.OptorSimMain} using the filename 
     * specified in the command line, or if none was specified, the
     * default of examples/parameters.conf is used.
     * @param filename The name of the parameters file.
     */
    public static void setFilename( String filename) {
	_parametersFilename = filename;	
    }

    private OptorSimParameters () {

	_table = new Properties();

	try {
	    FileInputStream fin = new FileInputStream( _parametersFilename);
	    _table.load(fin);
	    fin.close();
	}
	catch (Exception e) {
	    System.out.println("\nOptorSimParams> usage: Optor.csh/sh/bat [parameters file]");
	    System.out.println(" Please use a parameters file that exists or leave blank to use the default one.\n");
	    System.exit(1);
	}
    }

    /**
     * Gets the grid configuration file name.
     * */
    public String getGridConfigfile() {
	return _table.getProperty("grid.configuration.file");
    }

    /** Gets the job configuration file name.**/
    public String getJobConfigFile() {
	return _table.getProperty("job.configuration.file");
    }

    /** Gets the bandwidth configuration file name.*/
    public String getBandwidthConfigFile() {
	return (String)_table.getProperty("bandwidth.configuration.file");
    }

    /** Gets the number of jobs to be submitted.*/
    public int getNoOfJobs() {
	return Integer.parseInt(_table.getProperty("number.jobs"));
    }

    /** Gets the job scheduler to use.*/
    public int getScheduler() {
	return Integer.parseInt(_table.getProperty("scheduler"));
    }

    /** Gets the type of grid user which will submit jobs. Basically
     * this is the job submission pattern.
     */
    public int getUsers() {

        String users = _table.getProperty("users");

        if(users == null) return 1;
        return Integer.parseInt(users);
    }

    /** Gets the replica optimiser to use. */
    public int getOptimiser() {
	return Integer.parseInt(_table.getProperty("optimiser"));
    }

    /** Gets the time interval to use for the access histories.*/
    public long getDt() {
	return (long)(Long.parseLong(_table.getProperty("dt")));
    }

    /** Gets the kind of file access pattern to use.*/
    public int getAccessPatternGenerator() {
	return Integer.parseInt(_table.getProperty("access.pattern.generator"));
    }

    /** Gets the shape factor to use for the Zipf distribution.*/
    public double getShape() {
        return Double.parseDouble(_table.getProperty("shape"));
    }

    /** Gets the type of CE */
    public int getComputingElement() {
    	return Integer.parseInt(_table.getProperty("computing.element"));
    }
    
    /** Gets the fraction of the file set which will actually be 
     * requested by a job. That is, if the file set specified for a
     * job has 10 files and the
     * job set fraction is 0.5, the job will require 5 files every time
     * it runs. If the job set fraction is 2.5, the job will require 25
     * files every time it runs, and so on.
     */
 /*   public float getJobSetFraction() {
	return Float.parseFloat(_table.getProperty("job.set.fraction"));
    }*/

    /**
     * Gets the initial distribution of master files.
     */
    public String[] getFileDistribution() {
	String distribution = _table.getProperty("initial.file.distribution");
	if(distribution.indexOf(",") == -1) {
	    String[] sites = new String[1];
	    sites[0] = distribution;
	    return sites;
	}
	return distribution.split(",");
    }

    /** Checks whether all sites should be filled with files at the 
     * start or not.
     */
    public boolean fillAllSites() {
	if ((_table.getProperty("fill.all.sites")).equals("yes")) return true;
	return false;
    }

    /** Gets the delay between job submissions.*/
    public int getJobDelay() {
	return (int)(Integer.parseInt(_table.getProperty("job.delay")));
    }

    /** Checks whether or not to use a fixed random seed. 
     * @return true if a random seed should be used, false if a fixed seed
     * should be used.
     */
    public boolean useRandomSeed() {
	if ((_table.getProperty("random.seed")).equals("yes")) return true;
	return false;
    }

    /** Gets the maximum queue length for a ComputingElement.*/
    public int getMaxQueueSize() {
	return Integer.parseInt(_table.getProperty("max.queue.size"));
    }

    /** Gets the file processing time.*/
    public int getProcessTime() {

	String processTime = _table.getProperty("file.process.time");

	if(processTime == null) return 0;
	return (int)(Integer.parseInt(processTime));
    }

    /** Checks whether or not to use auctions.*/
    public boolean auctionOn() {
	if ((_table.getProperty("auction.flag")).equals("yes")) return true;
	return false;
    }

    /** Gets the number of hops a message should travel on the P2P system.*/
    public int getInitialHopCount() {
	return Integer.parseInt(_table.getProperty("hop.count"));
    }

    /** Gets the timeout value for parent auctions. Nested auctions
     * will have their timeout reduced by the Timeout Reduction Factor.*/
    public long getInitialTimeout() {
	return Long.parseLong(_table.getProperty("timeout"));
    }

    /** Gets the Timeout Reduction Factor for nested auctions. That is,
     * the factor by which the timeout for parent auctions should be 
     * reduced, so that nested auctions finish before the parent auctions.
     */
    public double getTimeoutReductionFactor() {
	return Double.parseDouble(_table.getProperty("timeout.reduction.factor"));
    }

    /** Check whether or not to output auction log information.*/
    public boolean useAuctionDebugger() {
	String debug = _table.getProperty("auction.log");
	if(debug == null || debug.equals("no")) return false;
	return true;
    }

    /** Check whether or not to use background traffic on the network.*/
    public boolean useBackgroundBandwidth() {
	String bw = _table.getProperty("background.bandwidth");
	if (bw == null || bw.equals("no")) return false;
	return true;
    }

    /** Gets the default background bandwidth file to use
     * when none other is available.*/
    public String getDefaultBackground() {
	return (String)_table.getProperty("default.background");
    }

    /** Gets the directory in which background bandwidth data files are kept.*/
    public String getDataDirectory() {
	return (String)_table.getProperty("data.directory");
    }

    /** Gets the time of day at which simulation time should start.
     * Should be on the hour or half hour.*/
    public float startTimeOfDay() {
	String timeOfDay = _table.getProperty("time.of.day");
	if( timeOfDay == null) return (float)0;
	return Float.parseFloat(timeOfDay);
    }

    /** Check whether or not to use the GUI*/
    public boolean useGui() {
	String gui = _table.getProperty("gui");
	if(gui == null || gui.equals("no")) return false;
	return true;
    }

    /** Gets the level of statistics output required. Default is 3,
     * i.e. full statistics output.*/
    public int outputStatistics() {
    	String stats = _table.getProperty("statistics");
    	if( stats == null || stats.equals("yes")) return 3;
        // any string apart from yes and numbers means no
        int i;
        try {
            i = Integer.parseInt(stats);
        } catch (NumberFormatException e) {
            return 1;
        }
        return i;
    }
    
    /** Check whether to use real time or event driven time model.
     */
    public boolean advanceTime() {
    	String adv = _table.getProperty("time.advance");
    	if( adv == null || adv.equals("no")) return false;
    	return true;
    }
}
