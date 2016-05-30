package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;
import org.jfree.data.XYSeries;
import org.jfree.data.DefaultPieDataset;

/**
 * This class creates the storage element instance 
 * thread which shall source statistical values at regular intervals
 * in real time from the storage elements statistics object.
 * 
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author Jamie Ferguson.
 */
public class SEDataThread extends Thread
{
   private String nodeChoice = "";
   private final int ONE_SECOND=1000;
   private int samplingDelay=1000;
   private GridContainer _gc = GridContainer.getInstance();
   private GridTime time = GridTimeFactory.getGridTime();
   private GridSite site;
   private int timeSecs=0;
   private long capacity=0;
   private boolean paused = false;
   private boolean printingAll = false;
   private static boolean processingJobs = true;
   private boolean passData = false;
   private float prevUsage = 0;
   private float prevPrevUsage = 0;
   
   private XYSeries seriesSEUVTime = new XYSeries("data for storage element");
   private DefaultPieDataset pieDataset = new DefaultPieDataset();
   /**
    * Constructor instantiates storage element object.
    * @param storElement String format of storage element,
    * essentially the name of this object.
    */
   public SEDataThread(String storElement)
   {
      nodeChoice = storElement;
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
    * Method to start the storage element thread.
    * This method accumulates data at regular intervals.
    */
   public void run()
   {
      //while (still executing jobs on grid)
      //    attempt to sample data
      while (processingJobs)
      {
         //if (not paused)
         //    sample data
         if (!paused)
         {
            //get the storage element object appropriate for this thread
            int ind = nodeChoice.indexOf('e');
            String siteno = nodeChoice.substring(ind+1);
            int siteID = Integer.parseInt(siteno);
            site = _gc.findGridSiteByID(siteID);
            StorageElement se = site.getSE();
            //sample time
            long timeMillis = time.getRunningTimeMillis();
            timeSecs = (int)(timeMillis/1000);
            //sample capacity
            capacity = se.getCapacity();
            float usage = (capacity - se.getAvailableSpace())/100;
            /* if (range values identical for last three readings)
             *    remove intermediate statistic
             */
            if (usage==prevUsage&&usage==prevPrevUsage)
            {    
               int itemCount = seriesSEUVTime.getItemCount();
               if (itemCount>2)
                  seriesSEUVTime.remove(itemCount-1);
            } 
            prevPrevUsage = prevUsage;
            prevUsage = usage;
            seriesSEUVTime.add(timeSecs, usage);
            pieDataset.setValue("Used Storage (GB)", new Integer((int)((capacity - se.getAvailableSpace())/100)));
            pieDataset.setValue("Free Storage (GB)", new Integer((int)((se.getAvailableSpace())/100)));
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
         OptorSimGUI.addStorSummaryTableValues(nodeChoice);
         OptorSimGUI.addStorUseGraph(seriesSEUVTime, nodeChoice);
         OptorSimGUI.addStorPieChart(pieDataset, nodeChoice);
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
