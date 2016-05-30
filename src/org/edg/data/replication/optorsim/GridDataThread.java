package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Enumeration;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

import org.jfree.data.XYSeries;

/**
 * This class creates the grid instance thread which 
 * shall source statistical values at regular intervals
 * in real time from the statistics objects of each of 
 * the elements present on this grid.
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author Jamie Ferguson.
 */
public class GridDataThread extends Thread
{
   private String nodeChoice = "Grid";
   private final int ONE_SECOND=1000;
   private int samplingDelay=1000;
   private GridContainer _gc = GridContainer.getInstance();
   private OptorSimParameters params = OptorSimParameters.getInstance();
   private GridSite site;
   private int timeSecs=0;
   private double [] histarray = new double [1];
   private int i=0;
   private int prevNoOfPairs=0;
   private boolean paused = false;
   private boolean printingAll = false;
   private static boolean processingJobs = true;
   private static int noOfJobs=0;
   private boolean passData = false;
   
   private XYSeries seriesGRFAVTime = new XYSeries("data for grid");
   private XYSeries seriesGLFAVTime = new XYSeries("data for grid");
   private XYSeries seriesGMJTVTime = new XYSeries("data for grid");
   private XYSeries seriesGJTHVTime = new XYSeries("data for grid");
   private XYSeries seriesGNRVTime = new XYSeries("data for grid");
   private XYSeries seriesGENUVTime = new XYSeries("data for grid");
   
   /**
    * Constructor instantiates grid object.
    */
   public GridDataThread()
   {
   }
   
   /**
    * Method to start the grid thread.
    * This method accumulates data at regular intervals.
    */
   public void run()
   {
      GridTime time = GridTimeFactory.getGridTime();
      //while (still executing jobs on grid)
      //    attempt to sample data
      while (processingJobs)
      {
         //if (not paused)
         //    sample data
         if (!paused)
         {
            int totalNoOfJobs=0;
            float totalJobTime=0;
            int totalRemoteReads=0;
            int totalLocalReads=0;
            int totalActiveCEs=0;
            float percFilled=0;
            float totalCapacity=0;
            float totalSpareCapacity=0;
            int pairs=0;
            
            //sampleTime
            long timeMillis = time.getRunningTimeMillis();
            timeSecs = (int)(timeMillis/1000);

            //cycle through all grid components sampling appropriat data
            for( Enumeration eSite = _gc.allGridSites(); eSite.hasMoreElements() ;)
            {
               GridSite gs = (GridSite) eSite.nextElement();
               if(gs.hasCE() || gs.hasSEs())
               {
                  if (gs.hasCE())
                  {
                     ComputingElement ce = gs.getCE();
                     Statistics st = ce.getStatistics();
                     
                     //sample number of jobs and sum
                     Object numberOfJobsObj = st.getStatistic("numberOfJobs");
                     String numberOfJobsStr = numberOfJobsObj.toString();
                     totalNoOfJobs += Integer.parseInt(numberOfJobsStr);
                     
                     //sample job times and sum
                     Object totalJobTimeObj = st.getStatistic("totalJobTime");
                     String totalJobTimeStr = totalJobTimeObj.toString();
                     totalJobTime += Float.parseFloat(totalJobTimeStr);
                     
                     //sample remote reads and sum
                     Object remoteReadsObj = st.getStatistic("remoteReads");
                     String remoteReadsStr = remoteReadsObj.toString();
                     totalRemoteReads += Integer.parseInt(remoteReadsStr);
                     
                     //sample local reads and sum
                     Object localReadsObj = st.getStatistic("localReads");
                     String localReadsStr = localReadsObj.toString();
                     totalLocalReads += Integer.parseInt(localReadsStr);
                     
                     //sample status and sum No. of active CEs
                     Object o = st.getStatistic("status");
                     String s = o.toString();
                     Boolean B =  new Boolean(s);
                     boolean b = B.booleanValue();
                     if (b)
                        totalActiveCEs++;
                     
                     //get the number of jobTime key value pairs and sum
                     Object r4 = st.getStatistic("jobTimes");
                     Map m = (Map)r4;
                     pairs += m.size(); 
                  }
                  if (gs.hasSEs())
                  {
                     //sample and sum capacity and available
                     StorageElement se = gs.getSE();
                     totalCapacity += se.getCapacity();
                     totalSpareCapacity += se.getAvailableSpace();
                  }
                  percFilled = (1-totalSpareCapacity/totalCapacity)*100;
               }
               
            }
            //get and set to a string the number of replications on grid
            Statistics gst = _gc.getStatistics();
            Object replicationsObj = gst.getStatistic("replications");
            String replicationsStr = replicationsObj.toString();
            float replicas = Float.parseFloat(replicationsStr);           
            //calculate the effective network usage
            float ENU = (totalRemoteReads + replicas)/(float)(totalLocalReads+totalRemoteReads);
            Float ENUFlo = new Float(ENU);
            //calculate the effective network usage
            float meanJTime = totalJobTime/totalNoOfJobs;
            
            //add latest data values to the various XYSeries
            if (!Float.isNaN(meanJTime))
            {
               seriesGMJTVTime.add(timeSecs, meanJTime);
               seriesGJTHVTime.add(timeSecs, meanJTime);
            }
            seriesGNRVTime.add(timeSecs, replicas);
            seriesGRFAVTime.add(timeSecs, totalRemoteReads);
            seriesGLFAVTime.add(timeSecs, totalLocalReads);
            if (!Float.isNaN(ENU))
               seriesGENUVTime.add(timeSecs, ENU);
            
            /* if (no of obTime key value pairs has increased)
             *      instantiate new array of doubles to be used in histogram
             *      cycle through all sites 
             *      if (site has a CE)
             *      while (site has more jobtime key value pairs)
             *          collating jobtimes
             */
            if (prevNoOfPairs!=pairs)
            {
               histarray = new double[pairs];
               int i=0;
               prevNoOfPairs +=pairs;
               for( Enumeration eSite = _gc.allGridSites(); eSite.hasMoreElements() ;)
               {
                  GridSite gs = (GridSite) eSite.nextElement();
                  if (gs.hasCE())
                  {
                     ComputingElement ce = gs.getCE();
                     Statistics st = ce.getStatistics();
                     Object r4 = st.getStatistic("jobTimes");
                     Map m = (Map)r4;
                     Set keySet = m.keySet();
                     Iterator iter = keySet.iterator();
                     while (iter.hasNext()&&i<pairs)
                     {
                        Object key = iter.next();
                        Object value = m.get(key);
                        String duration = value.toString();
                        float jobTime = Float.parseFloat(duration);
                        histarray[i] = jobTime;
                        i++;
                     }
                  }
               }
            }
            
            //if (all jobs completed)
            //    notify GUI
            if (totalNoOfJobs==noOfJobs)
            {
               OptorSimGUI.allJobsCompleted();
            }
            
            //if (not saving all graphs)
            //    try to refresh statistics
            if(!printingAll)
               this.sendDatatoGUI();
         }
         
         //delay next sample by short time
         try
         {
            if (paused)
               sleep(Integer.MAX_VALUE);
            else
               sleep(samplingDelay);
         }
         catch (InterruptedException e)
         {
            //in here if user is wishing to see chart from this object
            this.sendDatatoGUI();
         }
      }
      
      //out here only when all jobs have finished.
      //thread shall sleep for long time but can be 
      //re-awakened if user wants to see statistics 
      //from this object when run is complete.
      while (true)
      {
         try
         {
            sleep(Integer.MAX_VALUE);
         }
         catch (InterruptedException e)
         {
            //in here if user is wishing to see chart from this object
            this.sendDatatoGUI();
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
    * Method to rate of accumulation of data,
    * by setting the sampling delay.
    */
   public void setSampleRate(float rate)
   {
      samplingDelay = (int)(ONE_SECOND/rate);
   }
     
   /**
    * Method to establish whether user is saving all charts.
    * It does this by changing the printingAll status.
    */
   public void changePrintingAll()
   {
      printingAll = !printingAll;
   }
   
   /**
    * Method to determine whether user is 
    * attempting to view this objects statistics.
    * If they are not then it is pointless passing over 
    * the set of dataseries to GUI class
    * @param currViewingNode node on tree that user is currently viewing
    */
   public void currView(String currViewingNode)
   {
      if (nodeChoice.equals(currViewingNode))
         passData = true;
      else
         passData = false;
   }
   
   /**
    * Method to pass dataSeries over to the GUI class 
    * by calling relevant methods.
    */
   public void sendDatatoGUI()
   {
      //if (user is trying to view this objects statistics OR saving alll graphs)
      //    send data over to GUI class
      if (passData||printingAll)
      {
         OptorSimGUI.addGridSummaryTableValues(nodeChoice);
         OptorSimGUI.addGridMeanJobGraph(seriesGMJTVTime, nodeChoice);
         OptorSimGUI.addGridJobHistGraph(histarray, nodeChoice);
         OptorSimGUI.addGridNoRepGraph(seriesGNRVTime, nodeChoice);
         OptorSimGUI.addGridRemFilAccGraph(seriesGRFAVTime, nodeChoice);
         OptorSimGUI.addGridLocFilAccGraph(seriesGLFAVTime, nodeChoice);
         OptorSimGUI.addGridENUGraph(seriesGENUVTime, nodeChoice);
      }
   }
   
   /**
    * Method to set the number of jobs in simulation run. 
    * So when jobs completed = noOfJobs, GUI can be notified.
    * @param jobNumber number of jobs simulation has to execute
    */
   static public void setNoOfJobs(int jobNumber)
   {
      noOfJobs = jobNumber;
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
