package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

import java.util.Random;

/**
 * This class selects a file based on a Gaussian random walk: 
 * the sigma of which is set as half the number of files in
 * the set. The starting point for the random walk is the same
 * every time the same GridJob is run.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class RandomWalkGaussianAccessGenerator implements AccessPatternGenerator {
    private int _numFiles;
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

    public RandomWalkGaussianAccessGenerator(GridJob gridJob, 
					     float fileSetFraction) {

		_job = (GridJob)gridJob.clone();
		_numFiles = (int)((float)(_job.size()) * fileSetFraction);
		_fileCounter = 0;
		_fileId = 0;
    }

    /**
     * @return The next file is selected to be a random distance away
     * from the previous one, according to a Gaussian probability, whose
     * sigma is half the number of files in the set.
     */
    public String getNextFile() {
		int walkSteps; 

		if(_fileCounter<_numFiles) {

		    _fileCounter++;

		    // if there is only one file in the job
		    if(_job.size() == 1)
			return (String)_job.get(0);

		    // If this is the first file in the random walk:
		    if(_fileCounter==1) {
			_fileId = (int)(_startValue*_job.size());
		    }
		    else {
			// generate number to walk and direction
			walkSteps = (int)(_random.nextGaussian()*((double)_job.size())/2.0);
			_fileId += walkSteps;

			// wrap list of files round
			while(_fileId >= _job.size()) _fileId -= _job.size();
			while(_fileId < 0) _fileId += _job.size();
		    }
		    return (String)_job.get(_fileId);
		}
		return null;
    }

}
