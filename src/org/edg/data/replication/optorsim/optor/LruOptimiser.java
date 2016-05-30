package org.edg.data.replication.optorsim.optor;

import java.util.List;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;

/**
 * This optimiser replicates files at all times unless it is not possible
 * due to all files on the local SE being pinned or masters. To create space
 * for the replicated files it deletes the oldest files on the local SE,
 * that is, the files which were created least recently.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class LruOptimiser extends ReplicatingOptimiser {

    protected LruOptimiser( GridSite site) {
        super(site);
    }

    /**
     * Returns the DataFile chosen by {@link LruStorageElement#filesToDelete}.
     */
    protected List chooseFilesToDelete( DataFile file, StorageElement se) {
	    return se.filesToDelete(file);
    }

}
