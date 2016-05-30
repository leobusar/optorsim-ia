package org.edg.data.replication.optorsim;

import java.util.Random;

/**
 * These users sleep for a random length of time between 0 and
 * (2 * the job delay) milliseconds between submitting jobs.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or <a
 * href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
public class RandomWaitUsers extends SimpleUsers {

    private Random _rand;

    public RandomWaitUsers() {
        super();

        if( _params.useRandomSeed())
            _rand = new Random();
        else
            _rand = new Random(100L);
    }

    /**
     * Sleeps for a random number between 0 and 2 * job delay (so that
     * the average sleep is job delay)
     */
    public void waitForNextJob() {
  //      System.out.println("waiting "+_rand.nextInt(_params.getJobDelay() * 2));
        _time.gtSleep( _rand.nextInt(_params.getJobDelay() * 2));
    }

}
