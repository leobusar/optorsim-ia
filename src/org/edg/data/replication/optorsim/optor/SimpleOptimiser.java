package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.GridSite;

/**
 * This optimiser finds the 'best' replica of the required file but never
 * replicates, all files are read by remote I/O. All functionality is
 * provided by the methods in the SkelOptor superclass.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class SimpleOptimiser extends SkelOptor {

    protected SimpleOptimiser( GridSite site) {

	super(site);
    }

    protected SimpleOptimiser() {

	super();
    }        
}
