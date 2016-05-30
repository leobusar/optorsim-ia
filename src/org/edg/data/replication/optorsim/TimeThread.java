package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/**
 * This class creates the TimeThread instance thread which 
 * shall source the grid (or simulation) time at regular intervals
 * in real (or system) time from the gridTime instance.
 * 
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author Jamie Ferguson.
 */
public class TimeThread extends Thread
{
   final private int ONE_SECOND=1000;
   private boolean paused = false;
   private static boolean processingJobs = true;   
   
   /**
    * Constructor instantiates optorTimingClass.
    */
   public TimeThread()
   {
   }
   
   /**
    * Method to sample the grid (or simulation) 
    * time every second of real (or system) time
    * and call the appropriate static method in 
    * OptorSimGUI to refresh time display
    */
   public void run()
   {
      //get instance of gridtime
      GridTime time = GridTimeFactory.getGridTime();
      
      while(processingJobs)
      {
         //sample time
         OptorSimGUI.setTime(time.getDate());
         try
         {
            if (paused)
               sleep(Integer.MAX_VALUE);
            else
               sleep(ONE_SECOND);
         }
         catch (InterruptedException e)
         {
         }
      }
   }

   
   /**
    * Method to pause the accumulation of data,
    * by setting the paused variable to true.
    */
   public void pause()
   {
      paused = true;
   }
   
   /**
    * Method to unpause the accumulation of data,
    * by setting the paused variable to false.
    */
   public void unPause()
   {
      paused = false;
   }
   
   /**
    * Method to stop thread accumulating any more stats
    * by setting processingJobs to false.
    */
   static public void falsifyProcessingJobs()
   {
      processingJobs = false;
   }
}
