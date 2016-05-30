package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.GridJob;

/**
 * This class is the most basic AccessPatternGenerator, it
 * just hands the files back in the order they are in the
 * configuration file, starting with the first one.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class SequentialAccessGenerator implements AccessPatternGenerator {
    private int _numFiles;
    private GridJob _job;
    private int _fileId=0;
    private int _fileCounter=0;

    public SequentialAccessGenerator(GridJob gridJob, 
				     float fileSetFraction) {
		_job = gridJob;
		_numFiles = (int)( _job.size() * fileSetFraction);
    }

    /**
     * @return The next file according to the order in which they
     * were stated for this GridJob in the job configuration file.
     */
    public String getNextFile() {
			// file ID is filecounter mod numFilesInSet
		_fileId = _fileCounter - (_fileCounter / _job.size()) * _job.size();

		if(_fileCounter<_numFiles) {
			_fileCounter++;
			return (String) _job.get(_fileId);
		}

		return null;
    }
}
