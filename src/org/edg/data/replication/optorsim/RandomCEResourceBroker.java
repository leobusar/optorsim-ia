package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.optor.Optimisable;

import java.util.*;

/**
 * Schedules the GridJob to any random Computing Element that will
 * accept it.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class RandomCEResourceBroker extends SkelResourceBroker
{
    private Random _randGen;

    public RandomCEResourceBroker() {
        if( OptorSimParameters.getInstance().useRandomSeed())
            _randGen = new Random();
        else
            _randGen = new Random(100L);

    }

	public ComputingElement findCE( GridJob job, Optimisable optor) {

		List candidateCEs = new LinkedList();

		// build up a list of all those CEs that accept job.
		for( ListIterator li = GridContainer.getInstance().freeCEs(); li.hasNext();) {
		    ComputingElement ce = (ComputingElement)li.next();
		    if( ce.getSite().acceptsJob( job) && !ce.getJobHandler().isFull())
			candidateCEs.add(ce);
		}
		if( candidateCEs.size() == 0) return null;

		int index = _randGen.nextInt(candidateCEs.size());
		
		return (ComputingElement) candidateCEs.get( index);
	}
}
