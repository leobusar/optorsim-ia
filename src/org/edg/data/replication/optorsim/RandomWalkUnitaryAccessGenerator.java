package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import java.util.Random;

/**
 * This class selects a file based on a unitary random walk. This
 * means there is an equal chance of the next file being the previous
 * one or the next one in the list of files in the GridJob. The 
 * starting point for the random walk is the same every time the
 * same GridJob is run.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class RandomWalkUnitaryAccessGenerator implements AccessPatternGenerator {
    private int _numFiles;
    //private int _numFilesInSet;
    private GridJob _job;
    private int _fileCounter = 0;
    private int _fileId = 0;

	private final static Random _random;
	private final static boolean _randomSeed = OptorSimParameters.getInstance().useRandomSeed();
	// used to set the starting value of the random walk
	private final static float _startValue;

	static {
		if(_randomSeed) {
			_random = new Random();
			_startValue = (new Random()).nextFloat();
		} 
		else {
			_random = new Random(99);
			_startValue = (new Random(98)).nextFloat();	
		} 
	}

 
    public RandomWalkUnitaryAccessGenerator(GridJob gridJob, 
					    float fileSetFraction) {

		_job = (GridJob)gridJob.clone();
		_numFiles = (int)((float)(_job.size()) * fileSetFraction);
    }

    /**
     * @return The next file has an equal chance of being the previous
     * one or the next one in the list of files in the GridJob.
     */
    public String getNextFile() {
		boolean direction; 

		if(_fileCounter<_numFiles) {

	    	// if there is only one file in the job
	    	if(_job.size() == 1) {
				_fileCounter++;
				return (String)_job.get(0);
	    	}

		    // If this is the first file in the random walk:
		    if(_fileCounter==0) {
				_fileId = (int)(_startValue*_job.size());
	    	} else {
				// select direction of the walk (true => up)
				direction = _random.nextBoolean(); 
		
				if(direction) {
		    		_fileId++;
				} else {
		    		_fileId--;
				}

				// wrap list of files round
				if(_fileId >= _job.size()) _fileId -= _job.size();
				if(_fileId < 0) _fileId += _job.size();
	    	}

			_fileCounter++;
	    	return (String)_job.get(_fileId);
		}

		return null;
    }

}
