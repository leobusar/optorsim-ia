package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.auctions.AccessMediator;

import java.util.List;
import java.util.Iterator;

/**
 * This abstract class provides the getBestFile used by all
 * economic model optimisers. If the auction protocol is used
 * this method created a new Access Mediator to conduct each auction
 * and returns the results. If the auction protocol is not used,
 * getBestFile() of {@link ReplicatingOptimiser} is used and the
 * economic decision of whether to replicate is in each subclass'
 * implementation of chooseFileToDelete().
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
abstract public class EconomicModelOptimiser extends ReplicatingOptimiser {

    public EconomicModelOptimiser( GridSite site) {

	    super(site);
    }
    
    /**
     * Starts the auction process if it is to be used, or calls
     * ReplicatingOptimiser.getBestFile() otherwise. The economic decisions
     * are made in the subclasses' chooseFileToDelete() methods.
     */
    public DataFile[] getBestFile(String[] lfns, float[] fileFraction) {

        OptorSimParameters param = OptorSimParameters.getInstance();

        if( !param.auctionOn())
            return super.getBestFile( lfns, fileFraction);

        DataFile files[] = new DataFile [lfns.length];

        // Auction for each file.
        for(int i=0;i<lfns.length;i++) {
            files[i] = AccessMediator.getAM(_site).auction(lfns[i]);
        }

        return files;
    }

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
        return potentialFileWorth > deleteableFilesValue;
    }

}
