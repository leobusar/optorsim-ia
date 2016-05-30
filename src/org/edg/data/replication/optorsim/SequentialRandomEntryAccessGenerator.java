package org.edg.data.replication.optorsim;

import java.util.Random;

import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * This class is very similar to SequentialAccessGenerator. It
 * just hands the files back in the order they are in the
 * configuration file, but starts from a random point in the fileset rather
 * than the first file.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class SequentialRandomEntryAccessGenerator implements AccessPatternGenerator {
    private int _numFiles;
    private GridJob _job;
    private int _fileId=0;
    private int _fileCounter=0;

    private Random _random;
	private final static boolean _randomSeed = OptorSimParameters.getInstance().useRandomSeed();
	// used to set the starting value of the random walk
	private float _startValue;

    public SequentialRandomEntryAccessGenerator(GridJob gridJob, 
				     float fileSetFraction) {
		_job = gridJob;
		_numFiles = (int)( _job.size() * fileSetFraction);
		if(_randomSeed) {
			_random = new Random();
			_startValue = _random.nextFloat();
		}
		else {
			_random = new Random(99);
			_startValue = (new Random(98)).nextFloat();
		}
	}
  
    /**
     * @return The next file according to the order in which they
     * were stated for this GridJob in the job configuration file,
     * starting at the point given by _startValue.
     */
    public String getNextFile() {
    	 	
    	int start = (int)(_startValue*_job.size());
    	
			// file ID is filecounter mod numFilesInSet
		_fileId = start + _fileCounter - (_fileCounter / _job.size()) * _job.size();

//		 wrap list of files round
		if(_fileId >= _job.size()) _fileId -= _job.size();
		if(_fileId < 0) _fileId += _job.size();
		
		if(_fileCounter<_numFiles) {
			_fileCounter++;
			return (String) _job.get(_fileId);
		}

		return null;
    }
}
