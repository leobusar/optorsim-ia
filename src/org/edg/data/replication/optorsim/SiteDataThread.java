package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Map;
import java.util.Iterator;
import java.util.Set;

import org.jfree.data.XYSeries;

/**
 * This class creates the site instance thread which
 * shall source statistical values at regular intervals
 * in real time from the statistics objects of each of
 * the elements present at this site.
 * 
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author Jamie Ferguson.
 */
public class SiteDataThread extends Thread
{
   private String nodeChoice = "";
   private final int ONE_SECOND=1000;
   private int samplingDelay=1000;
   private GridContainer _gc = GridContainer.getInstance();
   private GridSite site;
   private int timeSecs=0;
   private double [] histarray = new double[1];
   private int prevNoOfPairs=0;
   private boolean paused = false;
   private boolean printingAll = false;
   private static boolean processingJobs = true;
   private boolean passData = false;
   private float prevCoUsage = 0;
   private float prevPrevCoUsage = 0;
   private float prevStUsage = 0;
   private float prevPrevStUsage = 0;
   
   private XYSeries seriesSMJTVTime = new XYSeries("data for site");
   private XYSeries seriesSJTHVTime = new XYSeries("data for site");
   private XYSeries seriesSCEUVTime = new XYSeries("data for site");
   private XYSeries seriesSSEUVTime = new XYSeries("data for site");
   
   /**
    * Constructor instantiates site object.
    * @param siteName String format of storage element,
    * essentially the name of this object.
    */
   public SiteDataThread(String siteName)
   {
      nodeChoice = siteName;
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
    * Method to start the site thread.
    * This method accumulates data at regular intervals.
    */
   public void run()
   {
      //get the gridsite instance
      int ind = nodeChoice.indexOf('e');
      String siteno = nodeChoice.substring(ind+1);
      int siteID = Integer.parseInt(siteno);
      site = _gc.findGridSiteByID(siteID);
      
      //while (still executing jobs on grid)
      //    attempt to sample data
      while (processingJobs)
      {
         /*if (not paused)
          *    sample time
          *    sample comp element data
          *    sample stor element data
          *    refresh graphs by calling appropriate method
          */
         if (!paused)
         {
            //sample time
            GridTime time = GridTimeFactory.getGridTime();
            long timeMillis = time.getRunningTimeMillis();
            timeSecs = (int)(timeMillis/1000);
            
            if (site.hasCE())
            {
               //in here only if site has a comp element
               ComputingElement ce = site.getCE();
               Statistics st = ce.getStatistics();
               Object o = st.getStatistic("runnableStatus");
               String s = o.toString();
               Boolean B =  new Boolean(s);
               boolean alive = B.booleanValue();
               //if (comp element still runnning)
               //    sample data
               if (alive)
               {
                  //get the statistics object for this comp. element
                  ce = site.getCE();
                  st = ce.getStatistics();
                  
                  //sample mean job time
                  Object r1 = st.getStatistic("meanJobTime");
                  String stat1 = r1.toString();
                  int stat1Int = Integer.parseInt(stat1);
                  seriesSMJTVTime.add(timeSecs, stat1Int);
                  
                  //sample job times
                  Object r2 = st.getStatistic("jobTimes");
                  Map m = (Map)r2;
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
                     while (iter.hasNext())
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
                  Object r3 = st.getStatistic("usage");
                  String stat3 = r3.toString();
                  float coUsage = Float.parseFloat(stat3);
                  /* if (range values identical for last three readings)
                   *    remove intermediate statistic
                   */
                  if (coUsage==prevCoUsage&&coUsage==prevPrevCoUsage)
                  {
                     int itemCount = seriesSSEUVTime.getItemCount();
                     if (itemCount>2)
                        seriesSSEUVTime.remove(itemCount-1);
                  }
                  prevPrevCoUsage = prevCoUsage;
                  prevCoUsage = coUsage;
                  seriesSCEUVTime.add(timeSecs, coUsage);
               }
            }
            
            if (site.hasSEs())
            {
               //in here only if site has a comp element
               StorageElement se = site.getSE();
               long capacity = se.getCapacity();
               float stUsage = (capacity - se.getAvailableSpace())/100;
               /* if (range values identical for last three readings)
                *    remove intermediate statistic
                */
               if (stUsage==prevStUsage&&stUsage==prevPrevStUsage)
               {
                  int itemCount = seriesSSEUVTime.getItemCount();
                  if (itemCount>2)
                     seriesSSEUVTime.remove(itemCount-1);
               }
               prevPrevStUsage = prevStUsage;
               prevStUsage = stUsage;
               seriesSSEUVTime.add(timeSecs, stUsage);
            }
            
            //if (not saving all graphs)
            //    try to refresh statistics
            if (!printingAll)
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
         OptorSimGUI.addSiteSummaryTableValues(nodeChoice);
         OptorSimGUI.addSiteMeanJobGraph(seriesSMJTVTime, nodeChoice);
         OptorSimGUI.addSiteJobHistGraph(histarray, nodeChoice);
         OptorSimGUI.addSiteCEUseGraph(seriesSCEUVTime, nodeChoice);
         OptorSimGUI.addSiteSEUseGraph(seriesSSEUVTime, nodeChoice);
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
