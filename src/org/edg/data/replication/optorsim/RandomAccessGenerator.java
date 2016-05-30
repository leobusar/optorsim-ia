package org.edg.data.replication.optorsim;

import java.util.Random;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * This access pattern generator selects files at random from the list in 
 * the {@link GridJob}.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class RandomAccessGenerator implements AccessPatternGenerator {
    private int _numFiles;
    private GridJob _job;
    private int _fileCounter=0;

	private final static Random _random;
	private final static boolean _randomSeed = OptorSimParameters.getInstance().useRandomSeed();

	static {
		if(_randomSeed) _random = new Random();
		else _random = new Random(99);
	}

    public RandomAccessGenerator(GridJob gridJob, 
				 float fileSetFraction) {
		_job = gridJob;
		_numFiles = (int)(_job.size() * fileSetFraction);
    }

    /**
     * @return The next random file picked from the list defined 
     * in the GridJob.
     */
    public String getNextFile() {
		int fileId;

		if(_fileCounter<_numFiles) {

		    _fileCounter++;

		    // if there is only one file in the job
		    if(_job.size() == 1)
			return (String)_job.get(0);

		    fileId = _random.nextInt(_job.size());
		    return  (String)_job.get(fileId);
		}
		return null;
    }

}
