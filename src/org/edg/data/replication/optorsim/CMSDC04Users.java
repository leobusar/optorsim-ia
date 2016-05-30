package org.edg.data.replication.optorsim;

/**
 * This class models the user patterns for analysis during the CMS
 * Data Challenege 2004 which ran from March to May. Information from
 * plots displayed at <a href="http://cmsdoc.cern.ch/cms/LCG/LCG-2/dc04/fakeanalysis/pierro/index.html">
 * http://cmsdoc.cern.ch/cms/LCG/LCG-2/dc04/fakeanalysis/pierro/index.html</a>
 * was used.
 * <p>
 * The user patterns of job submission for each day are modelled as a Gaussian
 * distribution centred around 3pm and the total number submitted each
 * day is just under 700.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or <a
 * href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
public class CMSDC04Users extends SimpleUsers {

    private static final int MAX_JOBS_PER_HOUR = 100;
    private static final int MS_IN_HOUR = 3600*1000;

    // the peak hour of the day for job submission
    private static final double PEAK_TIME = 15.0;
    // width of the Gaussian dist.
    private static final double SIGMA = 15.0;

    /**
     * Waits for a length of time dependent on how many jobs are
     * to be submitted this hour.
     */
    public void waitForNextJob() {

        int jobsInHour = jobsPerHour();
        while(jobsInHour == 0) {
            _time.gtSleep(MS_IN_HOUR);
            jobsInHour = jobsPerHour();
        }
        _time.gtSleep( MS_IN_HOUR/jobsInHour);
    }

    /**
     * Get the total number of jobs to be submitted this hour.
     */
    private int jobsPerHour() {
        int x = _time.getHours();
        return (int) (MAX_JOBS_PER_HOUR *
                (Math.exp(-(x-PEAK_TIME)*(x-PEAK_TIME)/SIGMA)));
    }

}
