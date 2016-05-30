package org.edg.data.replication.optorsim.infrastructure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.util.*;


/**
 * This class provides a method to read the contents of the job 
 * configuration file and other methods to access the retrieved information.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class JobConfFileReader
{
		/**
		 * We keep private copies here so we can easily look up jobs and files based
		 * on their names.  This isn't needed outside this class.
		 */
    private Map _jobCollection = new HashMap();
    private Map _fileCollection = new HashMap();
    private OptorSimParameters _params;

    private static JobConfFileReader _jobConfFileReader = null;

    /**
     * Returns the single instance of the JobConfFileReader, creating
     * it if it does not already exist.
     * @return The JobConfFileReader instance.
     */
    public static JobConfFileReader getInstance() {
		if( _jobConfFileReader == null)
	    	_jobConfFileReader = new JobConfFileReader();

		return _jobConfFileReader;
    }

    private JobConfFileReader() {
	_params = OptorSimParameters.getInstance();
	readFile( _params.getJobConfigFile() );
    }

	private void readFile( String filename) {
		GridContainer gc = GridContainer.getInstance();
		String tableName=null;
		FileInputStream fin=null;
		int state=0;
		
    	try {
    		fin = new FileInputStream( filename);
    	} catch( FileNotFoundException e) {
    		System.out.println( "JobConfFileReader> could not find file "+filename);
			System.exit(1); 
    	}
    	
    	System.out.println( "JobConfFileReader> reading file "+filename);

		BufferedReader in = new BufferedReader(new InputStreamReader(fin));
		StreamTokenizer st = new StreamTokenizer(in);
		
		st.commentChar( '#');
		st.eolIsSignificant( true);
		st.quoteChar('"');
		try {
			for( int tok; st.nextToken() != StreamTokenizer.TT_EOF; ) {

				switch( state) {
					
						// Looking for a command
					case 0:
						if( st.ttype != '\\') {
							state= (st.ttype == StreamTokenizer.TT_EOL)?0:1;
							break;
						}
						state=2;
						break;
						
						// Skip rest of line
					case 1:
						if( st.ttype == StreamTokenizer.TT_EOL)
							state=0;
						break;
						
						// just read a "\", looking for command
					case 2:
						if( st.ttype != StreamTokenizer.TT_WORD) {
							state= (st.ttype == StreamTokenizer.TT_EOL)?0:1;
							break;
						}
						if( st.sval.equals("begin")) {
							state=3;
							break;
						}
						if( ! st.sval.equals("end")) 
							System.out.println("Unknown command "+st.sval);
						state=1;
						break;
						
						// we're beginning some command, find out what.
					case 3:
						if( st.ttype != '{') {
							System.out.println("Syntax error at line "+st.lineno());
							state= (st.ttype == StreamTokenizer.TT_EOL)?0:1;
							break;							
						}
						if( st.nextToken() != StreamTokenizer.TT_WORD) {
							System.out.println("Syntax error at line "+st.lineno());
							state= (st.ttype == StreamTokenizer.TT_EOL)?0:1;
							break;							
						}
						String section=st.sval;
						if( st.nextToken() != '}') {
							System.out.println("Syntax error at line" +st.lineno());
							state= (st.ttype == StreamTokenizer.TT_EOL)?0:1;
						}
						if( section.equals("filetable")) {
							while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
									   (st.ttype != StreamTokenizer.TT_EOF));
							state=4;
						} else if( section.equals("jobtable")) {
							while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
									   (st.ttype != StreamTokenizer.TT_EOF));
							state=5;
							
						} else if( section.equals("cescheduletable")) {
							while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
									   (st.ttype != StreamTokenizer.TT_EOF));
							state=6;
							
						} else if( section.equals("jobselectionprobability")) {
							while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
									   (st.ttype != StreamTokenizer.TT_EOF));
							state=7;
						} else if( section.equals("jobexecutiontime")) {
							while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
									   (st.ttype != StreamTokenizer.TT_EOF));
							state=8;
						} else if( section.equals("filesetfraction")) {
							while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
									(st.ttype != StreamTokenizer.TT_EOF));
							state=9;
						} else
							state=1;
							
						break;
						
						// Reading a filetable entry
					case 4:
						if( st.ttype == '\\') {
							state=2;
							break;
						}
						if( st.ttype != StreamTokenizer.TT_WORD) {
							System.out.println( "Syntax error at line " +st.lineno());
							System.exit(1);
						}											
						String name = st.sval;
						if( st.nextToken() != StreamTokenizer.TT_NUMBER) {
							System.out.println( "Syntax error at line " +st.lineno());
							System.exit(1);
						}
						int size = (int) st.nval;
						if( st.nextToken() != StreamTokenizer.TT_NUMBER) {
							System.out.println( "Syntax error at line " +st.lineno());
							System.exit(1);
						}
						int index = (int) st.nval;
						
						_fileCollection.put( name,
			    					new DataFile(name, (int)size, index,
					 										false, true));

						while( (st.nextToken() != StreamTokenizer.TT_EOL) &&
								   (st.ttype != StreamTokenizer.TT_EOF));
						break;
						
						
						// Reading a jobtable entry
					case 5:
						if( st.ttype == '\\') {
							state=2;
							break;
						}
						if( st.ttype != StreamTokenizer.TT_WORD) {
							System.out.println( "Syntax error at line " +st.lineno());
							System.exit(1);
						}											
						String jobName = st.sval;	
						GridJob job = new GridJob(jobName);

						while( st.nextToken() == StreamTokenizer.TT_WORD) {
							if( _fileCollection.get( st.sval) != null)
								job.add( st.sval);
								//CN: quick fix, change back to original after I get results...
								//job.add((DataFile)(_fileCollection.get(st.sval)));
							else
								System.out.println( " Skipping unknown file "+ st.sval+ " at line "+st.lineno());
						}

						_jobCollection.put(jobName, job);

						while( (st.ttype != StreamTokenizer.TT_EOL) &&
								   (st.ttype != StreamTokenizer.TT_EOF))
							st.nextToken();
						break;


					// Reading a jobtable entry
				case 6:
					if( st.ttype == '\\') {
						state=2;
						break;
					}
					if( st.ttype != StreamTokenizer.TT_NUMBER) {
						System.out.println( "Syntax error at line " +st.lineno());
						System.exit(1);
					}
					GridSite site = gc.findGridSiteByID( (int) st.nval);
					if( site != null) {
						while( st.nextToken() == StreamTokenizer.TT_WORD) {
							job = (GridJob) _jobCollection.get(st.sval);
							if( job != null) 
								site.acceptJob( job);
							else
								System.out.println( "Skipping unknown job " +st.sval + " at line "+st.lineno());
						}
					} else
						System.out.println( "Skipping unknown site "+(int)st.nval+" at line "+st.lineno());
							// skip rest of line
					while( (st.ttype != StreamTokenizer.TT_EOL) &&
							   (st.ttype != StreamTokenizer.TT_EOF))
						st.nextToken();
					break;
					

					// Read probabilities
				case 7:
					if( st.ttype == '\\') {
						state=2;
						break;
					}
					if( st.ttype != StreamTokenizer.TT_WORD) {
						System.out.println( "Syntax error at line " +st.lineno());
						System.exit(1);
					}
					jobName = st.sval;

					if( st.nextToken() != StreamTokenizer.TT_NUMBER) {
						System.out.println( "Syntax error at line " +st.lineno());
						System.exit(1);
					}
					double prob = st.nval;

					job = (GridJob) _jobCollection.get(jobName);
					if( job != null)
						job.assignProbability( prob);
					else
						System.out.println( "Skipping unknown job " +jobName + " at line "+st.lineno());

							// skip rest of line
					while( (st.ttype != StreamTokenizer.TT_EOL) &&
							   (st.ttype != StreamTokenizer.TT_EOF))
						st.nextToken();
					break;
					// Read execution time parameters
				case 8:
                    if( st.ttype == '\\') {
                        state=2;
                        break;
                    }
                    if( st.ttype != StreamTokenizer.TT_WORD) {
                        System.out.println( "Syntax error at line " +st.lineno());
                        System.exit(1);
                    }											
                    jobName = st.sval;
                    job = (GridJob) _jobCollection.get(jobName);

                    if ( st.nextToken() == StreamTokenizer.TT_NUMBER)
                    {
  //                      System.out.println("DEBUG : JobName : "+jobName+" Latency :" + st.nval );
                        job.setLatency(st.nval) ;
                    }
                    else
                        System.out.println( " Syntax error at line "+st.lineno());

                    if ( st.nextToken() == StreamTokenizer.TT_NUMBER){
 //                       System.out.println("DEBUG : JobName : "+jobName+" Linear :" + st.nval );
                        job.setLinearFactor(st.nval) ;
                    }
                    else
                        System.out.println( " Syntax error at line "+st.lineno());
                    
                    while( (st.ttype != StreamTokenizer.TT_EOL) &&
                            (st.ttype != StreamTokenizer.TT_EOF))
                        st.nextToken();
                    break;
                    
                    //CN: Read fraction of file set to be accessed by each job type
				case 9: 
					if( st.ttype == '\\') {
                        state=2;
                        break;
                    }
                    if( st.ttype != StreamTokenizer.TT_WORD) {
                        System.out.println( "Syntax error at line " +st.lineno());
                        System.exit(1);
                    }											
                    jobName = st.sval;
                    job = (GridJob) _jobCollection.get(jobName);

                    if ( st.nextToken() == StreamTokenizer.TT_NUMBER)
                    {
  //                      System.out.println("DEBUG : JobName : "+jobName+" Latency :" + st.nval );
                        job.setFileFraction((float)st.nval) ;
                    }
                    else
                        System.out.println( " Syntax error at line "+st.lineno());

//                  Register the job, now that all the info is available.
                    gc.registerJob( job);
                    
                    while( (st.ttype != StreamTokenizer.TT_EOL) &&
                            (st.ttype != StreamTokenizer.TT_EOF))
                        st.nextToken();
                    break;
				}
			}			
		} catch ( IOException e) {
			System.out.println( "JobConfFileReader> failed to read file " + filename);
			System.exit(1);
		}
    }


     
    /**
     * Assign master files, based on assignment policy.  If requested, the SEs are filled random
     * copies of the file
     * @return Enumeration of all assigned DataFile objects.
     */
	public Iterator assignFilesToSites() {
		int siteIndex=0;
		GridContainer gc = GridContainer.getInstance();
		OptorSimParameters params = OptorSimParameters.getInstance();
		List assignedFiles = new Vector();
		
			/* We currently use a rather baroque method of specifying whether master files are randomly
			* distributed.  FileDistribution is either "random" implying randomly distributed over all sites.
			* or is a list of potential sites. */  
		String[] sites = params.getFileDistribution();
		boolean randomise = sites[0].equals("random");
	
		if( randomise)
			System.out.println("Assigning master files to random SEs");
		else
			System.out.println("Assigning master files to specified sites");

		int nPotentialSites = randomise ? gc.numberOfSites() : sites.length;
		
		// fill sites with the master files
		for(Iterator itr = _fileCollection.values().iterator(); itr.hasNext();) {			
			DataFile file = (DataFile)itr.next();
			DataFile storedFile = null;							
			int ok = 0;
			int perms[] = MathSupport.makePermutation( nPotentialSites); 
			
				// Try each potential site in turn.
			for( int i = 0; i < nPotentialSites; i++) {
			
				// chose random site index.  If not "random", chose randomly from those listed.
				//CN: modified to put full fileset at all specified sites.
//				siteIndex = (randomise) ?  perms[i] :  Integer.parseInt( sites[ perms[i]] );
				if (randomise) {
					siteIndex = perms[i];
					GridSite gsite = gc.findGridSiteByID( siteIndex); 
					storedFile = gsite.addFileToRandomSE( file);
					i = nPotentialSites;
					if( storedFile != null)
                                		assignedFiles.add( storedFile);
				} else {
					siteIndex = Integer.parseInt(sites[i]);
					GridSite gsite = gc.findGridSiteByID( siteIndex); 
					storedFile = gsite.addFileToRandomSE( file);
				//	System.out.println("Added file "+file.lfn()+" to site "+ gsite.toString()+ "with result "+storedFile);
					if( storedFile != null)
		                               assignedFiles.add( storedFile);

				}
			}
/**			if( storedFile != null)
				assignedFiles.add( storedFile);
       		else {
				System.out.println( "Error: not enough space in the Grid (or specified sites)"
						    +" to hold all the master files");
				System.exit(1); // TODO: proper exceptions for errors in optorsim
			}
**/		}
		
		// if we want to fill the SEs with replicas
		if( params.fillAllSites()) {
			
			System.out.println( "Filling remaining space with replicas");

			for( Enumeration eS = gc.allGridSites(); eS.hasMoreElements();) {
				GridSite gsite = (GridSite) eS.nextElement();

					// Attempt to fill all SEs at this site, then register them with the Replica Manager
				Iterator iFiles = gsite.fillSEsWithFiles( _fileCollection.values());
				while( iFiles.hasNext()) {
					DataFile file = (DataFile) iFiles.next();
					assignedFiles.add( file);
				}
			} // for each site
		    
		}  // if 
		
		return assignedFiles.iterator();
	}
    
}
