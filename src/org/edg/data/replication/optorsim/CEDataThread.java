package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Map;
import java.util.Iterator;
import java.util.Set;

import org.jfree.data.XYSeries;

/**
 * This class creates the computing element instance 
 * thread which shall source statistical values at regular intervals
 * in real time from the computing elements statistics object.
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author Jamie Ferguson.
 */
public class CEDataThread extends Thread
{
   private String nodeChoice = "";
   private final int ONE_SECOND=1000;
   private int samplingDelay=1000;
   private GridContainer gc = GridContainer.getInstance();
   private GridSite site;
   private int timeSecs=0;
   private Statistics st;
   private double [] histarray = new double[1];
   private int prevNoOfPairs=0;
   private boolean paused = false;
   private boolean printingAll = false;
   private static boolean processingJobs = true;
   private boolean passData = false;
   
   private XYSeries seriesRFAVTime = new XYSeries("data for computing element");
   private XYSeries seriesLFAVTime = new XYSeries("data for computing element");
   private XYSeries seriesMJTVTime = new XYSeries("data for computing element");
   private XYSeries seriesCEUVTime = new XYSeries("data for computing element");
   
   /**
    * Constructor instantiates computing element object.
    * @param compElement String format of computing element,
    * essentially the name of this object.
    */
   public CEDataThread(String compElement)
   {
      nodeChoice = compElement;
   }
   
   /** 
    * Method to return the name of this object in String format.
    * @return instance name.
    */
   public String getElement()
   {
      return nodeChoice;
   }
   
   /**
    * Method to start the computing element thread.
    * This method accumulates data at regular intervals.
    */
   public void run()
   {
      GridTime time = GridTimeFactory.getGridTime();
      
      //get the statistics object appropriate for this thread
      int ind = nodeChoice.indexOf('e');
      String siteno = nodeChoice.substring(ind+1);
      int siteID = Integer.parseInt(siteno);
      site = gc.findGridSiteByID(siteID);
      ComputingElement ce = site.getCE();
      System.out.println("CE Data Thread: CE "+ce.toString());
      st = ce.getStatistics();

      /* set alive to false
       * do 
       *    wait short period of time.
       *    check CE to see if it has been 
       *    enlivened by resource broker.
       * while CE remains unalive
       */
      Object o = st.getStatistic("runnableStatus");
      String s = o.toString();
      Boolean B =  new Boolean(s);
      boolean alive = B.booleanValue();
      do
      {
         try
         {
            sleep(ONE_SECOND);
            o = st.getStatistic("runnableStatus");
            s = o.toString();
            B =  new Boolean(s);
            alive = B.booleanValue();
         }
         catch (InterruptedException e)
         {}
      }
      while (!alive);
      
      //while (still executing jobs on grid)
      //    attempt to sample data
      while (processingJobs)
      {
         //if (not paused)
         //    sample data
         if (!paused)
         {
            o = st.getStatistic("runnableStatus");
            s = o.toString();
            B =  new Boolean(s);
            alive = B.booleanValue();
            
            if (alive)
            {
               siteID = Integer.parseInt(siteno);
               site = gc.findGridSiteByID(siteID);
               ce = site.getCE();
               st = ce.getStatistics();
               
               //sample time
               long timeMillis = time.getRunningTimeMillis();
               timeSecs = (int)(timeMillis/1000);

               //sample remote reads
               Object r1 = st.getStatistic("remoteReads");
               String stat1 = r1.toString();
               int stat1Int = Integer.parseInt(stat1);
               seriesRFAVTime.add(timeSecs, stat1Int);
               
               //sample local reads
               Object r2 = st.getStatistic("localReads");
               String stat2 = r2.toString();
               int stat2Int = Integer.parseInt(stat2);
               seriesLFAVTime.add(timeSecs, stat2Int);
               
               //sample mean job time
               Object r3 = st.getStatistic("meanJobTime");
               String stat3 = r3.toString();
               int stat3Int = Integer.parseInt(stat3);
               seriesMJTVTime.add(timeSecs, stat3Int);
               
               //sample job times
               Object r4 = st.getStatistic("jobTimes");
               Map m = (Map)r4;
               int pairs = m.size();
               //if (number of previous key-value pairs != pairs)
               //    instantiate new histarray and fill with job time values
               if (prevNoOfPairs!=pairs)
               {
                  histarray = new double[pairs];
                  int i=0;
                  prevNoOfPairs++;
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
               
               //sample usage
               Object r5 = st.getStatistic("usage");
               String stat5 = r5.toString();
               float stat5Int = Float.parseFloat(stat5);
               seriesCEUVTime.add(timeSecs, stat5Int);
               //if (not saving all graphs)
               //    try to refresh statistics
               if (!printingAll)
                  this.sendDatatoGUI();
            }
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
         OptorSimGUI.addCompSummaryTableValues(nodeChoice);
         OptorSimGUI.addCompMeanJobGraph(seriesMJTVTime, nodeChoice);
         OptorSimGUI.addCompJobHistGraph(histarray, nodeChoice);
         OptorSimGUI.addCompCEUseGraph(seriesCEUVTime, nodeChoice);
         OptorSimGUI.addCompRemFilAccGraph(seriesRFAVTime, nodeChoice);
         OptorSimGUI.addCompLocFilAccGraph(seriesLFAVTime, nodeChoice);
      }
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
