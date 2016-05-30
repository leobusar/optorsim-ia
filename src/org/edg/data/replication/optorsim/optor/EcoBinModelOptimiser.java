package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;

import java.util.List;

/**
 * This optimiser makes decisions on replication based on the
 * binomial economic model.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

public class EcoBinModelOptimiser extends EconomicModelOptimiser {

    protected EcoBinModelOptimiser( GridSite site) {

       super(site);
    }

    /**
     * Uses the algorithm contained in {@link EconomicBinomialStorageElement}
     * to determine whether replication should take place, and if so
     * which file should be deleted.
     */
    protected List chooseFilesToDelete( DataFile file, StorageElement se) {

        EconomicBinomialStorageElement thisSE = null;
        if( se instanceof EconomicBinomialStorageElement)
            thisSE = (EconomicBinomialStorageElement)se;
            
        double potentialFileWorth = thisSE.evaluateFileWorth(file.fileIndex()); 
        List deleteableFiles = thisSE.filesToDelete(file);
        
        if(worthReplicating(potentialFileWorth, deleteableFiles))
            return deleteableFiles;

        // not worth replicating so return null and do remote i/o
        return null;
    }	

}
