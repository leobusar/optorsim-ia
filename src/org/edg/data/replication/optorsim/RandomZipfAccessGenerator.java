package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.MathSupport;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
//import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.reptorsim.*;

/**
 * This class selects files using Zipf-like distribution.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class RandomZipfAccessGenerator implements AccessPatternGenerator {
    private int _numFiles;
    private GridJob _job;
    private int _fileCounter = 0;
    private double _shape;          // shape parameter of the distribution
    private int[] _permutation;
    private int _startValue = 0;
    
    public RandomZipfAccessGenerator(GridJob gridJob, 
				     float fileSetFraction) {

		_job = (GridJob)gridJob.clone();
		_numFiles = (int)((float)(_job.size()) * fileSetFraction);

		// Construct the sequence for zipf distribution
		_permutation = new int[_job.size()];
		//CN: file index does not necessarily start at 0
		String firstFilename = (String)gridJob.firstElement();
		ReplicaManager rm = ReplicaManager.getInstance();
		int startValue = (rm.listReplicas(firstFilename))[0].fileIndex();
		_startValue = startValue;
		for(int i=0; i<_job.size(); i++) _permutation[i] = i+startValue;
			
		// shape parameter of the distribution
		_shape = OptorSimParameters.getInstance().getShape();
    }

    /**
     * @return The next file which is determined by a Zipf-like
     * probability distribution.
     */
    public String getNextFile() {
		int fileId;

		if(_fileCounter<_numFiles) {
		    _fileCounter++;
	
		    // if there is only one file in the job
		    if(_job.size() == 1)
			return (String)_job.get(0);
		
		    fileId = MathSupport.zipfDistribution(_permutation, _shape);
		    return (String)_job.get(fileId-_startValue);
		}
		return null;
    }

}
