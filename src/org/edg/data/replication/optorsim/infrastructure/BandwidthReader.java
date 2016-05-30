package org.edg.data.replication.optorsim.infrastructure;

import java.io.*;
import java.util.*;

/**
 * This class provides a method to read the average available bandwidth for the
 * appropriate time of day from a file, for use in OptorSim as a "realistic" 
 * input bandwidth. 
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

public class BandwidthReader {
	
	static private BandwidthReader _instance = null;

	private Hashtable bandwidthDataHash = new Hashtable();

	
	/**
	 * Get an instance of the BandwidthReader. If none exists,
	 * instantiate a new BandwidthReader object; otherwise, use the
	 * existing object.
	 * @return the BandwidthReader instance.
	 */
	static public BandwidthReader getInstance() {
		if( _instance == null)
			_instance = new BandwidthReader();
			
		return _instance;
	}
	
	// Private constructor
	private BandwidthReader() {
		readConfigFile();
	}

    /* Method for reading the data file. Data files are of the form
     * time,mean bandwidth,standard deviation, with 49 entries . 
     * After reading, the data are stored in a hashtable,
     * where the link name (of form "sitei,sitej") is the key and the value is 
     * a vector containing the histogram of mean bandwidths and its errors.
 	 */
    private void readDataFile(String filename, GridSite site1, GridSite site2, Integer timeZone) {
    	String inputLine;
		Vector dataVec = new Vector(49); //one element for each half-hour bin + one for time zone
		dataVec.add(timeZone);
		GridSitePair gsp = new GridSitePair( site1, site2);

		try {
		    FileInputStream fin = new FileInputStream(filename);
		    BufferedReader in = new BufferedReader(new InputStreamReader(fin));
	    
		    while ((inputLine = in.readLine()) != null) {
		        inputLine=inputLine.trim();
				int linelength = inputLine.length();
				if(!inputLine.startsWith("#")) {
				    if(!inputLine.startsWith("\\")) { // begins with '\'
						int offset=inputLine.indexOf(",");
						int offset2 = inputLine.indexOf(",", offset+1);
						String tmp = inputLine.substring(offset+1, offset2);
						float mean = Float.parseFloat(tmp);
						tmp = inputLine.substring(offset2+1, linelength);
						float stdev = Float.parseFloat(tmp);
						dataVec.add(new BandwidthData(mean, stdev));
		    		}
				}
	    	}	

	    	in.close();
	    	fin.close();
		}
		catch(FileNotFoundException e) {
	    	System.out.println("BandwidthReader>  couldn't find file "+filename );
	    	System.exit(1);
		}
		catch( IOException e) {
			System.out.println( "IO Error: "+e);
		}
		bandwidthDataHash.put( gsp,dataVec);
    }

    /**
     * Get a BandwidthData object holding the mean and standard deviation
     * of the bandwidth between two sites at a given time.
     * @param site1 the source site
     * @param site2 the destination site
     * @param time the time of day for which to get the bandwidth
     * @return BandwidthData object holding mean and standard deviation
     * of the bandwidth for the given parameters.
     */
	public BandwidthData getBandwidth( GridSite site1, GridSite site2, float time) {
		GridSitePair gsp = new GridSitePair( site1, site2);

		Vector data = (Vector)bandwidthDataHash.get(gsp);
		if( data == null) {
			System.out.println( "BandwidthReader> no data for link between "+site1+" and "+site2);
			System.exit(1);
		}
		int timeZone = ((Integer)data.elementAt(0)).intValue();
		time += timeZone;    //account for different time zones
		if( time < 0)
			time += 24;
		if( time >= 24)
			time -= 24;	
		time *=2;  // integer between 0 and 48, to access elements of vector
		time +=1;  // first element of vector is time zone information

        return (BandwidthData)data.elementAt((int)time);
	}

    /** A method to get the correct data file for each link. 
     * Configuration file giving filenames corresponding to different links must be provided. 
     * If there are no data for a link, "-" should be entered in the configuration file and 
     * the default file (given in the parameters file) 
     * will be used. Histograms must be normalised. This method is called at the beginning and all 
     * data files read then, to minimise file i/o.
     */
    public void readConfigFile() {
	String configFile, inputLine, tmp, filename, defaultFile, dir, linkName;
	Integer timeZone;
	OptorSimParameters params = OptorSimParameters.getInstance();
	GridContainer gc = GridContainer.getInstance();
	Iterator jt = gc.iterateGridSites();

	int offset, oldoffset, linelength;

	configFile = params.getBandwidthConfigFile();
	dir = params.getDataDirectory();
	filename = defaultFile = params.getDefaultBackground();
	//above is default for when no other data are available	

	try {
	    FileInputStream fin = new FileInputStream(configFile);
	    BufferedReader in = new BufferedReader(new InputStreamReader(fin));
	    
	    while(( inputLine = in.readLine()) != null) {
		
		if(!inputLine.startsWith("#")) {
		    inputLine = inputLine.trim();
		    Iterator it = gc.iterateGridSites();
		    GridSite js = (GridSite)jt.next();
		    oldoffset = 0;
		    linelength = inputLine.length();
		    offset = inputLine.indexOf(" ");
		   
		    while(offset<linelength) {
			if(offset==(linelength-1)) {
			    tmp=inputLine.substring(oldoffset);
			}
			else {
			    tmp=inputLine.substring(oldoffset,offset);
			}
			linkName=tmp.substring(0,tmp.indexOf(","));			
			timeZone = Integer.valueOf(tmp.substring(tmp.indexOf(",")+1));  
			if(linkName.equals("-")) {
			    readDataFile(dir+defaultFile,js,(GridSite)it.next(), timeZone);
			}
			else  {
			    filename = (dir+linkName);
			    this.readDataFile(filename,js,(GridSite)it.next(), timeZone);
			}
			oldoffset = offset;
			oldoffset++;
			if(oldoffset<linelength) {
			    offset=inputLine.indexOf(" ",oldoffset);
			    if(offset==-1) offset=linelength-1;
			}
			else {
			    offset=linelength;
			}
		    }
		}
	    }
	    in.close();
	    fin.close();
	}
	catch(FileNotFoundException e) {
	    System.out.println("Error: "+filename+" not found");
	    System.exit(1);
	}
	catch(IOException e) {
		System.out.println( "IO Error: "+e);
	}
    }
}
