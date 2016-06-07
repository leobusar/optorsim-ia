package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.infrastructure.GridSite;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.auctions.AccessMediator;

import java.util.List;
import java.util.Iterator;

import java.util.List;

/**
 * This optimiser makes decisions on replication based on the
 * economic model, using the Zipf-based prediction function.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author leobusta
 * @since JDK1.4
 */

public class AcoOptimiser extends ReplicatingOptimiser {

    protected AcoOptimiser( GridSite site) {
	
	    super(site);
    }

    /**
     * Uses the algorithm contained in {@link AcoStorageElement}
     * to determine whether replication should take place, and if so
     * which file should be deleted.
     */
    protected List chooseFilesToDelete( DataFile file, StorageElement se) {

        AcoStorageElement thisSE = null;
        if( se instanceof AcoStorageElement)
            thisSE = (AcoStorageElement)se;
        System.out.println("**** PotentialWORTH  DeleteableValue "); 

        List deleteableFiles = thisSE.filesToDelete(file);
        if(worthReplicating(thisSE.evaluateFileWorth(file),
                deleteableFiles))
            return deleteableFiles;

        // not worth replicating so return null and do remote i/o
        return null;
    }

    /**
     * Starts the auction process if it is to be used, or calls
     * ReplicatingOptimiser.getBestFile() otherwise. The economic decisions
     * are made in the subclasses' chooseFileToDelete() methods.
     */
    public DataFile[] getBestFile(String[] lfns, float[] fileFraction) {

        OptorSimParameters param = OptorSimParameters.getInstance();
 //       System.out.println("GGG PotentialWORTH  DeleteableValue "); 

//        if( !param.auctionOn())
            return super.getBestFile( lfns, fileFraction);

/*        DataFile files[] = new DataFile [lfns.length];

        // Auction for each file.
        for(int i=0;i<lfns.length;i++) {
            files[i] = AccessMediator.getAM(_site).auction(lfns[i]);
        }

        return files;
*/    }

    /**
     * Tests whether the potential replica is more valuable
     * than the deleteable files already on the SE.
     * @param potentialFileWorth the value of the possible replica
     * @param deleteableFiles the list of deleteable files stored on the SE.
     * @return true if the file is more valuable than existing files, false if not.
     */
    protected boolean worthReplicating(double potentialFileWorth,
                                       List deleteableFiles) {

        double deleteableFilesValue = 0;
        for(Iterator i = deleteableFiles.iterator(); i.hasNext();) {
            DataFile file = (DataFile)i.next();
            deleteableFilesValue += file.lastEstimatedValue();
        }
        //TODO > or >=? >= would encourage more replication
//        System.out.println("**** PotentialWORTH "+ potentialFileWorth + " DeleteableValue " + deleteableFilesValue); 
        
/*        if(potentialFileWorth == deleteableFilesValue)
            if (Math.random()>0.5)
                return true;
*/            
        return potentialFileWorth > deleteableFilesValue;
    }

	
}

