package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.optor.OptimiserFactory;

import java.io.*;
import java.util.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.image.BufferedImage;
import javax.swing.*;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ChangeEvent;
import javax.swing.tree.DefaultMutableTreeNode;
import com.sun.image.codec.jpeg.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;



import org.jfree.chart.JFreeChart;
import org.jfree.chart.ChartFactory;
import org.jfree.data.XYSeries;
import org.jfree.data.XYSeriesCollection;
import org.jfree.data.XYDataset;
import org.jfree.data.DefaultPieDataset;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.chart.ChartUtilities;

/**
 * This class sets up the OptorSim GUI, which contains
 * components that facilitate user interaction
 * and display various statistical information.
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author Jamie Ferguson.
 */
public class OptorSimGUI extends JFrame implements TreeSelectionListener, ActionListener, ChangeListener, MouseListener, MouseMotionListener
{
   private static GridContainer gc = GridContainer.getInstance();
   private static OptorSimParameters params = OptorSimParameters.getInstance();
   private GridTime gt = GridTimeFactory.getGridTime();
   private static TimeThread timer = new TimeThread();
   private ResourceBroker rb;
   private static GridSite site;
   
   private static Graphics g;
   private static JPanel logic;
   private static String [][] currTransfers;
   private static String [][] tempArray;
   private static int currTransfersArrSize=0;
   private JLabel logicPic;
   
   private static boolean simPaused = true;
   
   private Container pane;
   private JMenuItem fMenuFileSelect, fMenuSaveGraphTable, fMenuProduceSummary, fMenuSaveParameters, fMenuExit;
   private JMenuItem simMenuStart, simMenuStop, simMenuPause, simMenuRestart;
   private JMenuItem statMenuSampleRate, statMenuGridHistNo, statMenuSiteHistNo, statMenuCompHistNo;
   private JMenuItem dMenuColNode, dMenuEstNode, dMenuColTopol, dMenuEstTopol, dMenuColRHS, dMenuEstTerm;
   private JMenuItem hMenuUserMan;
   private static JSplitPane leftSplitPane, splitPane;
   private static JScrollPane treeScroll, rightScroll, tableScroll;
   private static JScrollPane terminalScroll, ch;
   private static JTabbedPane tabbedPane;
   private JTree tree;
   private static JTextArea tabTerm;
   private static JTextField simulConverse, statsConverse, timeConverse;

   private static JFreeChart printableChart;
   private static String printableTitle = "";
   private static String overallSummaryText ="";
   
   private static int compTabSelected = -1;
   private static int storTabSelected = -1;
   private static int gridTabSelected = -1;
   private static int siteTabSelected = -1;
   
   private static int noOfSitesWithCompOrStor = 0;
   private static int totalNoOfCEs = 0;
   private static int totalNoOfSEs = 0;
   
   //no of histogram bars set at default value of 10
   private static int numOfGridBars = 10;
   private static int numOfSiteBars = 10;
   private static int numOfCompBars = 10;
   
   private boolean gridHistChange = false;
   private boolean siteHistChange = false;
   private boolean compHistChange = false;
   
   private JTable paramsTable;
   private static JPanel statsPanel;
   private static boolean printingAll = false;
   
   private static String treeChoice ="";
   
   private static GridDataThread griddt;
   private static SiteDataThread [] sitedt;
   private static CEDataThread [] cedt;
   private static SEDataThread [] sedt;
   
   private static int [] siteNumbers;
   private static int [][] coords;
   
   private boolean gridListen = false;
   private boolean siteListen = false;
   private boolean compListen = false;
   private boolean storListen = false;
   
   private boolean simulationRunning = false;
   private float frequency = 0;
   private static String timeNow = "";
   private JPopupMenu popup;
   
   private static JTabbedPane gridTabbedPane, siteTabbedPane, compTabbedPane, storTabbedPane;
   private static JPanel chartPanelg0 = new JPanel();
   private static JPanel chartPanelg1 = new JPanel();
   private static JPanel chartPanelg2 = new JPanel();
   private static JPanel chartPanelg3 = new JPanel();
   private static JPanel chartPanelg4 = new JPanel();
   private static JPanel chartPanelg5 = new JPanel();
   private static JPanel chartPanelg6 = new JPanel();
   
   private static JPanel chartPanels0 = new JPanel();
   private static JPanel chartPanels1 = new JPanel();
   private static JPanel chartPanels2 = new JPanel();
   private static JPanel chartPanels3 = new JPanel();
   private static JPanel chartPanels4 = new JPanel();
   
   private static JPanel chartPanelc0 = new JPanel();
   private static JPanel chartPanelc1 = new JPanel();
   private static JPanel chartPanelc2 = new JPanel();
   private static JPanel chartPanelc3 = new JPanel();
   private static JPanel chartPanelc4 = new JPanel();
   private static JPanel chartPanelc5 = new JPanel();
   
   private static JPanel chartPanelst0 = new JPanel();
   private static JPanel chartPanelst1 = new JPanel();
   private static JPanel chartPanelst2 = new JPanel();
   
   
   private static JTable gridTable;
   private static JTable siteTable;
   private static JTable compTable;
   private static JTable storTable;

   /** Main method to establish the Optorsim GUI.
     */
   public OptorSimGUI()
   {
      pane = this.getContentPane();
      this.setDefaultCloseOperation(EXIT_ON_CLOSE);
      this.setTitle("OptorSim User Interface");
      this.setSize(900, 725);
      this.setLocation(80,10);  // Initial displacement of gui from (0,0)
      this.layoutMenus();
      this.layoutPanels();
      this.show();
      this.setVisible(true);
      rb = ResourceBrokerFactory.getInstance();
   }
   
   /**
    * Method to layout the menubar on the GUI.
    * And set up actionListeners and
    * accelerator's for the menuItems.
    */
   private void layoutMenus()
   {
      //Establish menuBar
      JMenuBar menuBar = new JMenuBar();
      this.setJMenuBar(menuBar);
      menuBar.setBackground(new Color(0xffcc99));
      
      //Establish fileMenu and file menuItems
      JMenu fileMenu = new JMenu("File");
      menuBar.add(fileMenu);
      fileMenu.setBackground(new Color(0xffcc99));
      fileMenu.setMnemonic(KeyEvent.VK_F);
      fileMenu.getAccessibleContext().setAccessibleDescription(
      "File Menu");
      /*fMenuFileSelect = new JMenuItem("Select Configuration File");
      fileMenu.add(fMenuFileSelect);
      fMenuFileSelect.setBackground(new Color(0xffcc99));
      fMenuFileSelect.addActionListener(this);
      fMenuFileSelect.setAccelerator(KeyStroke.getKeyStroke(
      KeyEvent.VK_F, ActionEvent.CTRL_MASK));
      fileMenu.getAccessibleContext().setAccessibleDescription(
      "select a configuration file of your choice");*/
      fMenuSaveGraphTable = new JMenuItem("Save Graph / Summary Table");
      fileMenu.add(fMenuSaveGraphTable);
      fMenuSaveGraphTable.setBackground(new Color(0xffcc99));
      fMenuSaveGraphTable.setAccelerator(KeyStroke.getKeyStroke(
      KeyEvent.VK_G, ActionEvent.CTRL_MASK));
      fMenuSaveGraphTable.addActionListener(this);
      fMenuProduceSummary = new JMenuItem("Produce Summary");
      fileMenu.add(fMenuProduceSummary);
      fMenuProduceSummary.setBackground(new Color(0xffcc99));
      fMenuProduceSummary.setAccelerator(KeyStroke.getKeyStroke(
      KeyEvent.VK_U, ActionEvent.CTRL_MASK));
      fMenuProduceSummary.addActionListener(this);
      fMenuSaveParameters = new JMenuItem("Save Parameters Table");
      fileMenu.add(fMenuSaveParameters);
      fMenuSaveParameters.setBackground(new Color(0xffcc99));
      fMenuSaveParameters.setAccelerator(KeyStroke.getKeyStroke(
      KeyEvent.VK_P, ActionEvent.CTRL_MASK));
      fMenuSaveParameters.addActionListener(this);
      fMenuExit = new JMenuItem("Exit");
      fileMenu.add(fMenuExit);
      fMenuExit.setBackground(new Color(0xffcc99));
      fMenuExit.setAccelerator(KeyStroke.getKeyStroke(
      KeyEvent.VK_E, ActionEvent.CTRL_MASK));
      fMenuExit.addActionListener(this);
      
      //Establish simMenu and simulation menuItems
      JMenu simMenu = new JMenu("Simulation");
      menuBar.add(simMenu);
      simMenu.setBackground(new Color(0xffcc99));
      simMenu.setMnemonic(KeyEvent.VK_U);
      simMenu.getAccessibleContext().setAccessibleDescription(
      "Simulation Menu");
      simMenuStart = new JMenuItem("Start");
      simMenu.add(simMenuStart);
      simMenuStart.setBackground(new Color(0xffcc99));
      simMenuStart.addActionListener(this);
      simMenuStart.setAccelerator(KeyStroke.getKeyStroke("shift F1"));
      simMenuStop = new JMenuItem("Stop");
      simMenu.add(simMenuStop);
      simMenuStop.setBackground(new Color(0xffcc99));
      simMenuStop.addActionListener(this);
      simMenuStop.setAccelerator(KeyStroke.getKeyStroke("shift F2"));
      simMenuPause = new JMenuItem("Pause");
      simMenu.add(simMenuPause);
      simMenuPause.setBackground(new Color(0xffcc99));
      simMenuPause.addActionListener(this);
      simMenuPause.setAccelerator(KeyStroke.getKeyStroke("shift F3"));
      simMenuRestart = new JMenuItem("Restart");
      simMenu.add(simMenuRestart);
      simMenuRestart.setBackground(new Color(0xffcc99));
      simMenuRestart.addActionListener(this);
      simMenuRestart.setAccelerator(KeyStroke.getKeyStroke("shift F4"));
      
      //Establish statsMenu and statistics menuItems
      JMenu statsMenu = new JMenu("Statistics");
      menuBar.add(statsMenu);
      statsMenu.setBackground(new Color(0xffcc99));
      statsMenu.setMnemonic(KeyEvent.VK_S);
      statsMenu.getAccessibleContext().setAccessibleDescription(
      "Statistics Menu");
      statMenuSampleRate = new JMenuItem("Change Sample Rate");
      statsMenu.add(statMenuSampleRate);
      statMenuSampleRate.setBackground(new Color(0xffcc99));
      statMenuSampleRate.addActionListener(this);
      statMenuSampleRate.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_R,8));
      statMenuGridHistNo = new JMenuItem("Change No. of bins in Grid Histogram");
      statsMenu.add(statMenuGridHistNo);
      statMenuGridHistNo.setBackground(new Color(0xffcc99));
      statMenuGridHistNo.addActionListener(this);
      statMenuGridHistNo.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_G,8));
      statMenuSiteHistNo = new JMenuItem("Change No. of bins in Site Histograms");
      statsMenu.add(statMenuSiteHistNo);
      statMenuSiteHistNo.setBackground(new Color(0xffcc99));
      statMenuSiteHistNo.addActionListener(this);
      statMenuSiteHistNo.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S,8));
      statMenuCompHistNo = new JMenuItem("Change No. of bins in CE Histograms");
      statsMenu.add(statMenuCompHistNo);
      statMenuCompHistNo.setBackground(new Color(0xffcc99));
      statMenuCompHistNo.addActionListener(this);
      statMenuCompHistNo.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_C,8));
      
      //Establish displayMenu and display menuItems
      JMenu displayMenu = new JMenu("Display");
      menuBar.add(displayMenu);
      displayMenu.setBackground(new Color(0xffcc99));
      displayMenu.setMnemonic(KeyEvent.VK_D);
      displayMenu.getAccessibleContext().setAccessibleDescription(
      "Display Menu");
      dMenuColNode = new JMenuItem("Collapse Node Window");
      displayMenu.add(dMenuColNode);
      dMenuColNode.setBackground(new Color(0xffcc99));
      dMenuColNode.addActionListener(this);
      dMenuColNode.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N,3));
      dMenuColTopol = new JMenuItem("Collapse Topology Window");
      displayMenu.add(dMenuColTopol);
      dMenuColTopol.setBackground(new Color(0xffcc99));
      dMenuColTopol.addActionListener(this);
      dMenuColTopol.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_T,3));
      dMenuColRHS = new JMenuItem("Collapse Stats Window");
      displayMenu.add(dMenuColRHS);
      dMenuColRHS.setBackground(new Color(0xffcc99));
      dMenuColRHS.addActionListener(this);
      dMenuColRHS.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S,3));
      displayMenu.addSeparator();
      dMenuEstNode = new JMenuItem("Re-establish Node Window");
      displayMenu.add(dMenuEstNode);
      dMenuEstNode.setBackground(new Color(0xffcc99));
      dMenuEstNode.addActionListener(this);
      dMenuEstNode.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N,9));
      dMenuEstTopol = new JMenuItem("Re-establish Topology Window");
      displayMenu.add(dMenuEstTopol);
      dMenuEstTopol.setBackground(new Color(0xffcc99));
      dMenuEstTopol.addActionListener(this);
      dMenuEstTopol.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_T,9));
      dMenuEstTerm = new JMenuItem("Re-establish Stats Window");
      displayMenu.add(dMenuEstTerm);
      dMenuEstTerm.setBackground(new Color(0xffcc99));
      dMenuEstTerm.addActionListener(this);
      dMenuEstTerm.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S,9));
      
      //Establish helpMenu and help menuItems
      JMenu helpMenu = new JMenu("Help");
      menuBar.add(helpMenu);
      helpMenu.setBackground(new Color(0xffcc99));
      helpMenu.setMnemonic(KeyEvent.VK_H);
      helpMenu.getAccessibleContext().setAccessibleDescription(
      "Help Menu");
      hMenuUserMan = new JMenuItem("User Manual");
      helpMenu.add(hMenuUserMan);
      hMenuUserMan.setBackground(new Color(0xffcc99));
      hMenuUserMan.addActionListener(this);
      hMenuUserMan.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_M,3));
      
   }
   
   /**
    * Method to layout the panels on the GUI
    * and place components on those panels.
    */
   private void layoutPanels()
   {
      //construct tree (TLHS of container)
      DefaultMutableTreeNode root = new DefaultMutableTreeNode("Grid");
      this.createCategories(root);
      tree = new JTree(root);
      treeScroll = new JScrollPane(tree);
      treeScroll.setVerticalScrollBarPolicy(
      JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
      treeScroll.setHorizontalScrollBarPolicy(
      JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
      
      //ensures that selected tabs on GUI are highlighted chosen color
      UIManager.put("TabbedPane.selected", new Color(0xffffcc));
      
      //construct tabs (BLHS of container)
      tabbedPane = new JTabbedPane();
      tabbedPane.setBackground(new Color(0xcc6699));
      
      //first tab - parameters table
      //use this opportunity whilst getting lots of stats
      //from optorSimParameters set some variables
      //Total number of Jobs
      GridDataThread.setNoOfJobs(params.getNoOfJobs());
      //sample frequency
      frequency = 1;
      
      String fileDistArray [] = params.getFileDistribution();
      int fileDistArraySize = fileDistArray.length;
      String fileDistribution = fileDistArray[0];
      for (int i=1; i<fileDistArraySize; i++)
      {
         fileDistribution += " ," + fileDistArray[i];
      }
      String[] columnNames =
      {"Parameter", "Value"};
      Object[][] data =
      {
         {"Grid Configuration File",params.getGridConfigfile()},
         {"Job Configuration File",params.getJobConfigFile()},
         {"Bandwidth Configuration File",params.getBandwidthConfigFile()},
         {"Number of jobs", new Integer(params.getNoOfJobs())},
         
         {"Scheduler", new Integer(params.getScheduler())},
         {"Optimiser", new Integer(params.getOptimiser())},
         {"Optimisation Algorithm", OptimiserFactory.getOptAlgo()},
         {"Access Pattern", AccessPatternGeneratorFactory.getAccessPatternName()},
         {"Access History Length (ms)", new Long(params.getDt())},
         {"Zipf distribution shape", new Double(params.getShape())},
 //        {"Job set fraction", new Float(params.getJobSetFraction())},
         
         {"Initial File Distribution",fileDistribution},
         {"Fill all Sites?", new Boolean(params.fillAllSites())},
         {"Job Delay (ms)", new Integer(params.getJobDelay())},
         {"Random Seed?", new Boolean(params.useRandomSeed())},
         {"Max. Queue Size", new Integer(params.getMaxQueueSize())},
 //        {"File Process Time (ms)", new Integer(params.getProcessTime())},
         
         {"Auction", params.auctionOn()?"On":"Off"},
         {"Hop Count", new Integer(params.getInitialHopCount())},
         {"Auction Initial Timeout (ms)", new Long(params.getInitialTimeout())},
         {"Auction Timeout Reduction Factor", new Float(params.getTimeoutReductionFactor())},
         {"Auction Log", new Boolean(params.useAuctionDebugger())},
         
         {"Background Bandwidth", new Boolean(params.useBackgroundBandwidth())},
         {"Data Directory", new String(params.getDataDirectory())},
         {"Default Background", new String(params.getDefaultBackground())},
         {"Start Time", new Float(params.startTimeOfDay())},
         
        {"Statistics level", new Integer(params.outputStatistics())}
         
      };
      paramsTable = new JTable(data, columnNames);
      tableScroll = new JScrollPane(paramsTable);
      
      tableScroll.setVerticalScrollBarPolicy(
      JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
      tabbedPane.addTab("Parameters",null, tableScroll,
      "Displays Parameters");
      tabbedPane.setMnemonicAt(0, KeyEvent.VK_P);
      
      //second tab - (terminal) standard output
      tabTerm = new JTextArea();
      tabTerm.setBackground(Color.black);
      tabTerm.setForeground(Color.white);
      terminalScroll = new JScrollPane(tabTerm);
      terminalScroll.setVerticalScrollBarPolicy(
      JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
      terminalScroll.setHorizontalScrollBarPolicy(
      JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
      tabbedPane.addTab("Terminal",null, terminalScroll,
      "Displays terminal output");
      tabbedPane.setMnemonicAt(1, KeyEvent.VK_T);
      
      //third tab - node diagram of grid. This schematic
      //shall also display network traffic in real time
      logic = new JPanel();
      logic.setBackground(new Color(0xcc99ff));
      BufferedImage logicImage = new BufferedImage(250, 250,BufferedImage.TYPE_INT_RGB);
      g = logicImage.createGraphics();
      //g.setColor(new Color(0x000000));
      g.setColor(new Color(0x3399ff));
      g.fillRect(0,0, 250,250);
      g.setColor(Color.red);
      coords = new int[noOfSitesWithCompOrStor][2];
      for (int i =0; i<noOfSitesWithCompOrStor; i++)
      {
         int pX = (int) (125 + 100 * Math.cos(Math.toRadians((i * 360.0 / noOfSitesWithCompOrStor)-90)));
         int pY = (int)(125 + 100 * Math.sin(Math.toRadians((i * 360.0 / noOfSitesWithCompOrStor)-90)));
         g.fillOval(pX, pY, 6,6);
         coords[i][0] = pX;
         coords[i][1] = pY;
      }
      Graphics2D g2d = (Graphics2D)g;
      g2d.setColor(Color.black);
      g2d.translate(125,125);
      //for loop to draw rest of the nodes around the circle outline spaced out equally
      for (int i =0; i<noOfSitesWithCompOrStor; i++)
      {
         double x=0;
         double y=-110;
         int xO= (int)x;
         int yO=(int)y;
         String st = String.valueOf(siteNumbers[i]);
         if (st.length()==1)
            xO = 0;
         else
            xO = (-100/2)*st.length()/noOfSitesWithCompOrStor;
         g2d.drawString(st,xO,yO);
         g2d.rotate(6.28/noOfSitesWithCompOrStor);
      }
      g2d.translate(-125,-125);
      logicPic = new JLabel();
      logicPic.setIcon(new ImageIcon(logicImage));
      logic.add(logicPic);
      ch = new JScrollPane(logic);
      tabbedPane.addTab("Logical View",null, ch,
      "Logical schema of the grid");
      tabbedPane.setMnemonicAt(2, KeyEvent.VK_L);
      
      //Establish the tree scrollPane and the tabbed pane
      //onto vertically cleaved JSplitPane
      leftSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT,true);
      leftSplitPane.setTopComponent(treeScroll);
      leftSplitPane.setBottomComponent(tabbedPane);
      leftSplitPane.setResizeWeight(0.5);
      leftSplitPane.setOneTouchExpandable(true);
      
      
      //Establish statsPane (RHS of container)
      statsPanel = new JPanel();
      statsPanel.setBackground(new Color(128,128,255));
      rightScroll = new JScrollPane(statsPanel);
      BorderLayout bl = new BorderLayout();
      statsPanel.setLayout(bl);
      
      
      //add a JTextField for statistics feedback to top of statsPanel
      statsConverse = new JTextField();
      statsConverse.setEditable(false);
      statsConverse.setBackground(new Color(0xff6600));
      statsPanel.add(statsConverse,"North");
      
      timeConverse = new JTextField();
      statsPanel.add(timeConverse,"South");
      
      //Establish the left splitPane and the statsPane
      //onto a horizontally cleaved JSplitPane
      splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,true);
      splitPane.setLeftComponent(leftSplitPane);
      splitPane.setRightComponent(rightScroll);
      splitPane.setDividerLocation(300);
      splitPane.setResizeWeight(0.5);
      splitPane.setOneTouchExpandable(true);
      pane.add(splitPane,"Center");
      
      
      //add a JTextField for simulation feedback to bottom of container
      simulConverse = new JTextField();
      simulConverse.setEditable(false);
      simulConverse.setText("Press Simulation -> Start to begin.");
      simulConverse.setBackground(new Color(0xff6600));
      pane.add(simulConverse,"South");
      
      
      //set colour of stats panels now
      //to avoid flickering when viewing later
      colourStatsPanels();
   }
   
   /**
    * Method to set the background colour of all the stats panels
    */
   static private void colourStatsPanels()
   {
      chartPanelg0.setBackground(Color.blue);
      chartPanelg1.setBackground(Color.blue);
      chartPanelg2.setBackground(Color.blue);
      chartPanelg3.setBackground(Color.blue);
      chartPanelg4.setBackground(Color.blue);
      chartPanelg5.setBackground(Color.blue);
      chartPanelg6.setBackground(Color.blue);
      
      chartPanels0.setBackground(Color.blue);
      chartPanels1.setBackground(Color.blue);
      chartPanels2.setBackground(Color.blue);
      chartPanels3.setBackground(Color.blue);
      chartPanels4.setBackground(Color.blue);
      
      chartPanelc0.setBackground(Color.blue);
      chartPanelc1.setBackground(Color.blue);
      chartPanelc2.setBackground(Color.blue);
      chartPanelc3.setBackground(Color.blue);
      chartPanelc4.setBackground(Color.blue);
      chartPanelc5.setBackground(Color.blue);
      
      chartPanelst0.setBackground(Color.blue);
      chartPanelst1.setBackground(Color.blue);
      chartPanelst2.setBackground(Color.blue);
   }
   
   /**
    * Method to set mouselistener for popup menus on stats pane JPanels.
    */
   private void addPopupListeners()
   {
      chartPanelg0.addMouseListener(this);
      chartPanelg1.addMouseListener(this);
      chartPanelg2.addMouseListener(this);
      chartPanelg3.addMouseListener(this);
      chartPanelg4.addMouseListener(this);
      chartPanelg5.addMouseListener(this);
      chartPanelg6.addMouseListener(this);
      
      chartPanels0.addMouseListener(this);
      chartPanels1.addMouseListener(this);
      chartPanels2.addMouseListener(this);
      chartPanels3.addMouseListener(this);
      chartPanels4.addMouseListener(this);
      
      chartPanelc0.addMouseListener(this);
      chartPanelc1.addMouseListener(this);
      chartPanelc2.addMouseListener(this);
      chartPanelc3.addMouseListener(this);
      chartPanelc4.addMouseListener(this);
      chartPanelc5.addMouseListener(this);
      
      chartPanelst0.addMouseListener(this);
      chartPanelst1.addMouseListener(this);
      chartPanelst2.addMouseListener(this);
   }
   
   /**
    * Method to display in real time the traffic between the nodes
    * on the logical schema of the grid.
    * @param from site files are coming from.
    * @param to site files are going to.
    * @param noOfFiles number of files that are being transferred.
    */
   static public void filesTransferred(GridSite from, GridSite to, int noOfFiles)
   {
      //get the site number of the source
      String site1 = from.toString();
      int e1 = site1.indexOf('e');
      int num1 = Integer.parseInt(site1.substring(e1+1));
      
      //get the site number of the destination
      String site2 = to.toString();
      int e2 = site2.indexOf('e');
      int num2 = Integer.parseInt(site2.substring(e2+1));
      
      //get the coordinates of the two nodes
      int XFrom=0, YFrom=0, XTo=0, YTo=0;
      //source
      for (int i=0; i<noOfSitesWithCompOrStor; i++)
      {
         if (siteNumbers[i]==num1)
         {
            XFrom = coords[i][0];
            YFrom = coords[i][1];
            i=noOfSitesWithCompOrStor;
         }
      }
      //destination
      for (int i=0; i<noOfSitesWithCompOrStor; i++)
      {
         if (siteNumbers[i]==num2)
         {
            XTo = coords[i][0];
            YTo = coords[i][1];
            i=noOfSitesWithCompOrStor;
         }
      }
      
      /**
       * if (nodes already transfering files)
       *    increment no. of transfers between these sites
       *    then do nothing else vis a vis logic diagram
       * if (nodes not already transferring files)
       *    copy array of transferring files record to temp array
       *    increment no. of transfers between these sites
       *    instantiate new current transfers array (one bigger than previous)
       *    copy contents of temp array into curr transfers array
       *    add transfer as final row in curr transfers array
       *    draw line on logic diagram showing transfer
       */
      int connections =0;
      boolean transferFound = false;
      for (int i=0;i<currTransfersArrSize;i++)
      {
         if (site1.equals(currTransfers[i][0])&&site2.equals(currTransfers[i][1]))
         {
            int t = Integer.parseInt(currTransfers[i][2]);
            t++;
            currTransfers[i][2] = String.valueOf(t);
            i = currTransfersArrSize;
            connections=i;
            transferFound = true;
         }
      }
      if (!transferFound)
      {
         tempArray = new String [currTransfersArrSize][3];
         for (int i=0;i<currTransfersArrSize;i++)
         {
            tempArray[i][0] = currTransfers[i][0];
            tempArray[i][1] = currTransfers[i][1];
            tempArray[i][2] = currTransfers[i][2];
         }
         currTransfersArrSize++;
         currTransfers = new String [currTransfersArrSize][3];
         for (int i=0;i<currTransfersArrSize-1;i++)
         {
            currTransfers[i][0] = tempArray[i][0];
            currTransfers[i][1] = tempArray[i][1];
            currTransfers[i][2] = tempArray[i][2];
         }
         currTransfers[currTransfersArrSize-1][0] = site1;
         currTransfers[currTransfersArrSize-1][1] = site2;
         currTransfers[currTransfersArrSize-1][2] = "1";
         connections = 1;
         
      }
      g.setColor(Color.red);
      g.drawLine(XFrom,YFrom,XTo,YTo);
      logic.repaint();
      logic.updateUI();
   }
    
   /**
    * Method to remove in real time tha traffic that has ceased
    * between the nodes on the logical schema of the grid.
    * @param from site files were coming from.
    * @param to site files were going to.
    * @param noOfFiles number of files that were being transferred.
    */
   static public void fileTransferStopped(GridSite from, GridSite to, int noOfFiles)
   {
      //get the site number of the source
      String site1 = from.toString();
      int e1 = site1.indexOf('e');
      int num1 = Integer.parseInt(site1.substring(e1+1));
      
      //get the site number of the destination
      String site2 = to.toString();
      int e2 = site2.indexOf('e');
      int num2 = Integer.parseInt(site2.substring(e2+1));
      
      //get the coordinates of the two nodes
      int XFrom=0, YFrom=0, XTo=0, YTo=0;
      //source
      for (int i=0; i<noOfSitesWithCompOrStor; i++)
      {
         if (siteNumbers[i]==num1)
         {
            XFrom = coords[i][0];
            YFrom = coords[i][1];
            i=noOfSitesWithCompOrStor;
         }
      }
      //destination
      for (int i=0; i<noOfSitesWithCompOrStor; i++)
      {
         if (siteNumbers[i]==num2)
         {
            XTo = coords[i][0];
            YTo = coords[i][1];
            i=noOfSitesWithCompOrStor;
         }
      }
      
      /* if (a transfer is taking place between source and destination)
       *    get the number of transfers for that record.
       *    if (no of transfers = 1)
       *       delete that record by moving all
       *       subsequent transfers up by one.
       *       decrement current transfer array size.
       *       instantiate new temp array.
       *       copy current transfer array to temp array.
       *       instantiate new current transfer array.
       *       copy temp array to current transfer array.
       *       draw line (same colour as background)
       *       between source and destination.
       * else
       *    deccrement no of transfers for that record.
       */
      for (int i=0;i<currTransfersArrSize;i++)
      {
         if (site1.equals(currTransfers[i][0])&&site2.equals(currTransfers[i][1]))
         {
            int t = Integer.parseInt(currTransfers[i][2]);
            if (t==1)
            {
               for (int j=i; j<currTransfersArrSize-1; j++)
               {
                  currTransfers[j][0] = currTransfers[j+1][0];
                  currTransfers[j][1] = currTransfers[j+1][1];
                  currTransfers[j][2] = currTransfers[j+1][2];
               }
               currTransfersArrSize--;
               tempArray = new String [currTransfersArrSize][3];
               for (int j=0;j<currTransfersArrSize;j++)
               {
                  tempArray[j][0] = currTransfers[j][0];
                  tempArray[j][1] = currTransfers[j][1];
                  tempArray[j][2] = currTransfers[j][2];
               }
               currTransfers = new String [currTransfersArrSize][3];
               for (int j=0;j<currTransfersArrSize;j++)
               {
                  currTransfers[j][0] = tempArray[j][0];
                  currTransfers[j][1] = tempArray[j][1];
                  currTransfers[j][2] = tempArray[j][2];
               }
               g.setColor(new Color(0x3399ff));
               g.drawLine(XFrom,YFrom,XTo,YTo);
               logic.repaint();
               logic.updateUI();
            }
            else
            {
               t--;
               currTransfers[i][2] = String.valueOf(t);
            }
         }
      }
   }
   
   /**
    * Method to create nodes on tree (categories and subcategories)
    * based on network layout info.
    * @param root root node (grid as a whole).
    */
   public void createCategories(DefaultMutableTreeNode root)
   {
      DefaultMutableTreeNode category;    //sites
      DefaultMutableTreeNode subCategory; //elements
      
      /* Cycle through all gridsites
       * if (gridsite har a comp or stor element)
       *    add site to tree (could have added routers also by
       *    moving "category = new DefaultMutableTreeNode(gs);"
       *    up out of if loop).
       *    add CE subcategory if necessary.
       *    add SE subcategory if necessary.
       */
      for( Enumeration eSite = gc.allGridSites(); eSite.hasMoreElements() ;)
      {
         GridSite gs = (GridSite) eSite.nextElement();
         if(gs.hasCE() || gs.hasSEs())
         {
            noOfSitesWithCompOrStor++;
            category = new DefaultMutableTreeNode(gs);
            root.add(category); //add site to grid container
            if (gs.hasCE())
            {
               ComputingElement ce = gs.getCE();
               subCategory = new DefaultMutableTreeNode(ce.toString(),false);
               category.add(subCategory); //add CE to site
               totalNoOfCEs++;
            }
            if (gs.hasSEs())
            {
               StorageElement se = gs.getSE();
               subCategory = new DefaultMutableTreeNode(se.toString(),false);
               category.add(subCategory); //add SE to site
               totalNoOfSEs++;
            }
         }
      }
      //create arrays of thread objects, one for each node
      //except root as that was created previously
      sitedt = new SiteDataThread[noOfSitesWithCompOrStor];
      cedt = new CEDataThread[totalNoOfCEs];
      sedt = new SEDataThread[totalNoOfSEs];
      
      //create array of site numbers. Do this now as we know how
      //large array has to be when instantiated.
      siteNumbers = new int[noOfSitesWithCompOrStor];
      int siteNumbersIndex = 0;
      for( Enumeration eSite = gc.allGridSites(); eSite.hasMoreElements() ;)
      {
         GridSite gs = (GridSite) eSite.nextElement();
         if(gs.hasCE() || gs.hasSEs())
         {
            String site = gs.toString();
            int e = site.indexOf('e');
            int num = Integer.parseInt(site.substring(e+1));
            siteNumbers[siteNumbersIndex] = num;
            siteNumbersIndex++;
         }
      }
   }
   
   /**
    * Method to initiate the appropriate response to an
    * actionevent (menu option chosen or text returned from JTextField).
    * @param e the actionevent fired.
    */
   public void actionPerformed(ActionEvent e)
   {
//////////////////////////File menu button pressed
   /*   if (e.getSource()==fMenuFileSelect)
      {
         // Create a new frame
         Frame f = new Frame();
         // Create a file dialog
         FileDialog fdialog = new FileDialog
         ( f , "Select Configuration File", FileDialog.LOAD);
         // Show frame
         fdialog.show();
         // Clean up and exit
         fdialog.dispose();
         f.dispose();
      }
      
      else*/
       if (e.getSource()==fMenuSaveGraphTable)
      {
         /* if (viewing grid summary table)
          *    save grid summary table
          * else if (viewing site summary table)
          *    save site summary table
          * else if (viewing CE summary table)
          *    save CE summary table
          * else if (viewing SE summary table)
          *    save SE summary table
          * else
          *    save graph that is being viewed
          */
         if (gridListen&&gridTabSelected==0)
         {
            Dimension size = gridTable.getSize();
            BufferedImage myImage =
            new BufferedImage(size.width, size.height,
            BufferedImage.TYPE_INT_RGB);
            Graphics2D g2 = myImage.createGraphics();
            gridTable.paint(g2);
            try
            {
               OutputStream out = new FileOutputStream(printableTitle +"@"+ timeNow+".JPEG");
               JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
               encoder.encode(myImage);
               out.close();
            }
            catch (Exception exc)
            {
               System.out.println(exc);
            }
         }
         else if (siteListen&&siteTabSelected==0)
         {
            Dimension size = siteTable.getSize();
            BufferedImage myImage =
            new BufferedImage(size.width, size.height,
            BufferedImage.TYPE_INT_RGB);
            Graphics2D g2 = myImage.createGraphics();
            siteTable.paint(g2);
            try
            {
               OutputStream out = new FileOutputStream(printableTitle +"@"+ timeNow+" .JPEG");
               JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
               encoder.encode(myImage);
               out.close();
            } catch (Exception exc)
            {
               System.out.println(exc);
            }
         }
         else if (compListen&&compTabSelected==0)
         {
            Dimension size = compTable.getSize();
            BufferedImage myImage =
            new BufferedImage(size.width, size.height,
            BufferedImage.TYPE_INT_RGB);
            Graphics2D g2 = myImage.createGraphics();
            compTable.paint(g2);
            try
            {
               OutputStream out = new FileOutputStream(printableTitle +"@"+ timeNow+".JPEG");
               JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
               encoder.encode(myImage);
               out.close();
            }
            catch (Exception exc)
            {
               System.out.println(exc);
            }
         }
         else if (storListen&&storTabSelected==0)
         {
            Dimension size = storTable.getSize();
            BufferedImage myImage =
            new BufferedImage(size.width, size.height,
            BufferedImage.TYPE_INT_RGB);
            Graphics2D g2 = myImage.createGraphics();
            storTable.paint(g2);
            try
            {
               OutputStream out = new FileOutputStream(printableTitle +"@"+ timeNow+".JPEG");
               JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
               encoder.encode(myImage);
               out.close();
            }
            catch (Exception exc)
            {
               System.out.println(exc);
            }
         }
         else if (!gridListen&&!siteListen&&!compListen&&!storListen)
         {
            JOptionPane.showMessageDialog(null, "No chart has been selected!");
         }
         else
         {
            try
            {
               ChartUtilities.saveChartAsJPEG(new File(printableTitle +"@"+ timeNow+".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
               ioe.printStackTrace();
            }
         }
      }
      else if (e.getSource()==fMenuProduceSummary)
      {   
         /*if (simulation paused)
          *    write out data to produce an overall summary
          *else
          *    display information message
          */
         if (simPaused)
         {
            simulConverse.setText("Saving all graphs. This shall take a few moments. Saving complete when file drop down menu collapses.");
            //make new directory where all JPEGs stored
            File newDir = new File("allGraphs@"+ timeNow+"/");
            newDir.mkdir(); //Creates a directory called allGraphs
            
            //initialise xhtml text with standard header and add title 
            //reference to cascading style sheet etc.
            overallSummaryText = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">\n" +
            "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n\n");
            overallSummaryText += ("<head>\n<title>Final Overall Summary</title>\n");
            overallSummaryText += ("<link rel=\"stylesheet\" type=\"text/css\" href=\"summary.css\" />\n");
            overallSummaryText += ("</head>\n\n<body>\n");
            overallSummaryText += ("<p><a href=\"#grid\">Grid Statistics</a></p>\n");
            overallSummaryText += ("<p><a href=\"#sites\">Site Statistics</a></p>\n");
            overallSummaryText += ("<p><a href=\"#storage\">Storage Element Statistics</a></p>\n");
            overallSummaryText += ("<p><a href=\"#computing\">Computing Elements Statistics</a></p>\n");
            
            //alert all grid components that all their graphs are about to be saved
            griddt.changePrintingAll();
            for (int i = 0;i<noOfSitesWithCompOrStor;i++)
            {
               sitedt[i].changePrintingAll();
               sedt[i].changePrintingAll();
            }
            
            for (int i = 0;i<totalNoOfCEs;i++)
            {
               cedt[i].changePrintingAll();
            }
            
            //alter variable so that the appropriate methods in this class 
            //are aware that all graphs are getting saved
            printingAll = true;
            
            //append xhtml text with grid graphs and grid info
            overallSummaryText += ("<p><a name=\"grid\"></a></p>\n");
            overallSummaryText += ("<h1>Grid</h1>\n");
            griddt.sendDatatoGUI();
            overallSummaryText += ("<p><a name=\"sites\"></a></p>\n");
            overallSummaryText += ("<h1>Sites</h1>\n");
            //append xhtml text with all sites graphs and sites info
            for (int o = 0;o<noOfSitesWithCompOrStor;o++)
            {
               sitedt[o].sendDatatoGUI();
            }
            //append xhtml text with all SEs graphs and SEs info
            overallSummaryText += ("<p><a name=\"storage\"></a></p>\n");
            overallSummaryText += ("<h1>Storage Elements</h1>\n");
            for (int o = 0;o<noOfSitesWithCompOrStor;o++)
            {
               sedt[o].sendDatatoGUI();
            }
            //append xhtml text with all CEs graphs and CEs info
            overallSummaryText += ("<p><a name=\"computing\"></a></p>\n");
            overallSummaryText += ("<h1>Computing Elements</h1>\n");
            for (int o = 0;o<totalNoOfCEs;o++)
            {
               cedt[o].sendDatatoGUI();
            }
            overallSummaryText += ("\n</body>\n</html>");
            
            //alter variable so that the appropriate methods in this class 
            //are aware that all graphs are finished getting saved
            printingAll = false;
            
            //alert all grid components that their graphs are finished getting saved
            griddt.changePrintingAll();
            for (int i = 0;i<noOfSitesWithCompOrStor;i++)
            {
               sitedt[i].changePrintingAll();
               sedt[i].changePrintingAll();
            }
            for (int i = 0;i<totalNoOfCEs;i++)
            {
               cedt[i].changePrintingAll();
            }
            
            //write out files to working directory
            try
            {
               //write the xhtml file - overall summary
               FileWriter fw = new FileWriter("Summary@"+ timeNow+".html");
               PrintWriter pw = new PrintWriter(fw);
               pw.println(overallSummaryText);
               pw.close();
               
               //write the cascading style sheet to be used with overall summary
               fw = new FileWriter("summary.css");
               pw = new PrintWriter(fw);
               pw.println("h1{font-size: 36pt}\nh2{font-size: 12pt;color: #ff6666}");
               pw.close();
               
            }
            catch (IOException exc)
            {
               System.out.println(exc);
            }
         }
         else
         {
            JOptionPane.showMessageDialog(null, "Simulation must be paused before printing overall summary.");
         }
      }
      else if (e.getSource()==fMenuSaveParameters)
      {
         //save the parameters table to parameters.JPEG
         Dimension size = paramsTable.getSize();
         BufferedImage myImage =
         new BufferedImage(size.width, size.height,
         BufferedImage.TYPE_INT_RGB);
         Graphics2D g1 = myImage.createGraphics();
         paramsTable.paint(g1);
         try
         {
            OutputStream out = new FileOutputStream("parameters.JPEG");
            JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
            encoder.encode(myImage);
            out.close();
         }
         catch (Exception exc)
         {
            System.out.println(exc);
         }
      }
      else if (e.getSource()==fMenuExit)
      {
         //close down the program (and hence the container)
         System.exit(0);
      }
      
//////////////////////////Simulation menu button pressed     
      else if (e.getSource()==simMenuStart)
      {
         /* if (simulation not running)
          *    start all activities
          * else
          *    show appropriate error message   
          */
         if(!simulationRunning)
         {
            //now that simulation has started listeners can be activated
            tree.addTreeSelectionListener(this);
            logicPic.addMouseListener(this);
            logicPic.addMouseMotionListener(this);
            this.addPopupListeners();
            
            //start the resource broker and the timing classes
            rb.unPauseRB();
            gt.start();
            timer.start();
            simPaused = false;
            simulationRunning = true;
            simulConverse.setText("Simulation currently running.");
            
            int noOfSites = 0;
            int noOfCEs = 0;
            int noOfSEs = 0;
            //cycle through all of the grids components
            //setting up the threads and
            //setting the sample rate
            
            //set up grid thread
            griddt = new GridDataThread();
            griddt.setSampleRate(frequency);
            griddt.start();
            for( Enumeration eSite = gc.allGridSites(); eSite.hasMoreElements() ;)
            {
               DefaultMutableTreeNode subCategory;
               GridSite gs = (GridSite) eSite.nextElement();
               if(gs.hasCE() || gs.hasSEs())
               {
                  //in here to set up a site thread
                  String gSite = gs.toString();
                  sitedt[noOfSites] = new SiteDataThread(gSite);
                  sitedt[noOfSites].setSampleRate(frequency);
                  sitedt[noOfSites].start();
                  noOfSites++;
                  if (gs.hasCE())
                  {
                     //in here to set up a computing element thread
                     ComputingElement ce = gs.getCE();
                     String compEle = ce.toString();
                     cedt[noOfCEs] = new CEDataThread(compEle);
                     cedt[noOfCEs].setSampleRate(frequency);
                     cedt[noOfCEs].start();
                     noOfCEs++;
                  }
                  if (gs.hasSEs())
                  {
                     //in here to set up a storage element thread
                     StorageElement se = gs.getSE();
                     String storEle = se.toString();
                     sedt[noOfSEs] = new SEDataThread(storEle);
                     sedt[noOfSEs].setSampleRate(frequency);
                     sedt[noOfSEs].start();
                     noOfSEs++;
                  }
               }
            }
         }
         else
         {
            JOptionPane.showMessageDialog(null, "Simulation has already been initiated!");
         }
      }
      else if (e.getSource()==simMenuStop)
      {
         /* if (simulation running)
          *    stop all activities
          * else
          *    show appropriate error message   
          */
         if (simulationRunning)
         {
            //pause the resource broker and computing 
            //elements - the active components of the grid
            //and the timing classes
            rb.pauseRB();
            gc.pauseAllCEs();
            gt.stop();
            timer.pause();
            simPaused = true;
            simulationRunning = false;
            simulConverse.setText("Simulation stopped.");
         }
         else
         {
            JOptionPane.showMessageDialog(null, "Simulation has not been initiated!");
         }
         
      }
      else if (e.getSource()==simMenuPause)
      {
         /* if (simulation running & not paused)
          *    pause all activities
          * else
          *    show appropriate error message   
          */
         if (simulationRunning&&!simPaused)
         {
            //pause the resource broker and computing 
            //elements - the active components of the grid
            //and the timing classes
            rb.pauseRB();
            gc.pauseAllCEs();
            gt.stop();
            timer.pause();
            simPaused = true;
            simulConverse.setText("Simulation Paused. To Restart, Press Simulation -> Restart.");
            
            //pause all the grid component threads
            griddt.pause();
            for (int i = 0;i<noOfSitesWithCompOrStor;i++)
            {
               sitedt[i].pause();
               sedt[i].pause();
            }
            for (int i = 0;i<totalNoOfCEs;i++)
            {
               cedt[i].pause();
            }
            simulConverse.setText("Simulation Paused. To Restart, Press Simulation -> Restart.");
         }
         else if (!simulationRunning)
         {
            JOptionPane.showMessageDialog(null, "Simulation has not been initiated!");
         }
         else
         {
            JOptionPane.showMessageDialog(null, "Simulation already paused!");
         }
         
      }
      else if (e.getSource()==simMenuRestart)
      {
         /* if (simulation running & paused)
          *    re-enliven all activities
          * else
          *    show appropriate error message   
          */
         if (simulationRunning&&simPaused)
         {
            rb.unPauseRB();
            gc.unpauseAllCEs();
            gt.start();
            timer.unPause();
            timer.interrupt();
            simPaused = false;
            
            //wake all grid components sleeping threads
            griddt.unPause();
            griddt.interrupt();
            for (int i = 0;i<noOfSitesWithCompOrStor;i++)
            {
               sitedt[i].unPause();
               sitedt[i].interrupt();
               sedt[i].unPause();
               sedt[i].interrupt();
            }
            for (int i = 0;i<totalNoOfCEs;i++)
            {
               cedt[i].unPause();
               cedt[i].interrupt();
            }
            simulConverse.setText("Simulation currently running.");
         }
         else if (!simPaused)
         {
            JOptionPane.showMessageDialog(null, "Simulation already running!");
         }
         else
         {
            JOptionPane.showMessageDialog(null, "Simulation has not been initiated!");
         }
      }
      
//////////////////////////Statistics menu button pressed    
      else if (e.getSource()==statMenuSampleRate)
      {
         statsConverse.setText("Enter desired Sample Rate in Hertz and press return. Frequency = ");
         statsConverse.setEditable(true);          //permits text input to statsconverse
         statsConverse.requestFocus();             //sets the keyboard focus
         statsConverse.addActionListener(this);    //listen for events in stats converse
      }
      else if (e.getSource()==statMenuGridHistNo)
      {
         statsConverse.setText("Enter No of Bars desired for Grid Histogram and press return. No of Bars = ");
         statsConverse.setEditable(true);          //permits text input to statsconverse
         statsConverse.requestFocus();             //sets the keyboard focus
         statsConverse.addActionListener(this);    //listen for events in stats converse
         //alter booleans to indicate which histogram shall change
         gridHistChange = true;
         siteHistChange = false;
         compHistChange = false;
      }
      else if (e.getSource()==statMenuSiteHistNo)
      {
         statsConverse.setText("Enter No of Bars desired for Site Histogram and press return. No of Bars = ");
         statsConverse.setEditable(true);          //permits text input to statsconverse
         statsConverse.requestFocus();             //sets the keyboard focus
         statsConverse.addActionListener(this);    //listen for events in stats converse
         //alter booleans to indicate which histogram shall change
         gridHistChange = false;
         siteHistChange = true;
         compHistChange = false;
      }
      else if (e.getSource()==statMenuCompHistNo)
      {
         statsConverse.setText("Enter No of Bars desired for Comp Histogram and press return. No of Bars = ");
         statsConverse.setEditable(true);          //permits text input to statsconverse
         statsConverse.requestFocus();             //sets the keyboard focus
         statsConverse.addActionListener(this);    //listen for events in stats converse
         //alter booleans to indicate which histogram shall change
         gridHistChange = false;
         siteHistChange = false;
         compHistChange = true;
      }
      else if (e.getSource()==statsConverse)
      {
         //set statsconverse back to default state
         statsConverse.removeActionListener(this); 
         treeScroll.requestFocus();                //remove keyboard focus from statsConverse
         statsConverse.setEditable(false);
         //get the number entered into statsconverse
         String barsText = statsConverse.getText();
         int u = barsText.indexOf('=');
         String num  = barsText.substring(u+2);
         
         /* if (find which histograms to be changed)
          *    change the appropriate variable
          *    give user feedback
          *    interrupt the appropriate thread
          * else
          *    must be sampling frequency to be changed
          */
         if (gridHistChange)
         {
            numOfGridBars = Integer.parseInt(num);
            statsConverse.setText("Number of bars in Grid Histogram have now changed to " + numOfGridBars);
            griddt.interrupt();
         }
         else if (siteHistChange)
         {
            numOfSiteBars = Integer.parseInt(num);
            statsConverse.setText("Number of bars in Site Histograms have now changed to " + numOfSiteBars);
            int index = 0;
            for (int i=0; i<noOfSitesWithCompOrStor; i++)
            {
               String nodeCheck = sitedt[i].getElement();
               if (nodeCheck.equals(treeChoice))
               {
                  index = i;
                  i = noOfSitesWithCompOrStor;
               }
            }
            sitedt[index].interrupt();
         }
         else if (compHistChange)
         {
            numOfCompBars = Integer.parseInt(num);
            statsConverse.setText("Number of bars in Computing element Histograms have now changed to " + numOfCompBars);
            int index = 0;
            for (int i=0; i<totalNoOfCEs; i++)
            {
               String nodeCheck = cedt[i].getElement();
               if (nodeCheck.equals(treeChoice))
               {
                  index = i;
                  i = totalNoOfCEs;
               }
            }
            cedt[index].interrupt();
         }
         else
         {
            frequency = Float.parseFloat(num); //changes sample rate before running
            if (simulationRunning)             //changes sample rate if already running
            {
               griddt.setSampleRate(frequency);
               for (int i = 0;i<noOfSitesWithCompOrStor;i++)
               {
                  sitedt[i].setSampleRate(frequency);
                  sedt[i].setSampleRate(frequency);
               }
               for (int i = 0;i<totalNoOfCEs;i++)
               {
                  cedt[i].setSampleRate(frequency);
               }
            }
            statsConverse.setText("Sampling Frequency has now changed to " + frequency + "Hz.");
         }
        
         //set boolean variables back to default
         gridHistChange = false;
         siteHistChange = false;
         compHistChange = false;
      }
      
//////////////////////////Display menu button pressed
      //removes TL (node) window
      else if (e.getSource()==dMenuColNode)
      {
         leftSplitPane.setDividerLocation(0);
      }
      
      //removes BL (topology) window
      else if (e.getSource()==dMenuColTopol)
      {
         leftSplitPane.setDividerLocation(1.0);
      }
      
      //removes RHS (or stats) window
      else if (e.getSource()==dMenuColRHS)
      {
         splitPane.setDividerLocation(1.0);
      }
      //re-establishes TL (node) window
      else if (e.getSource()==dMenuEstNode)
      {
         leftSplitPane.setDividerLocation(0.5);
      }
      
      //re-establishes BL (topology) window
      else if (e.getSource()==dMenuEstTopol)
      {
         leftSplitPane.setDividerLocation(0.5);
      }
      
      //re-establishes RHS (or stats) window
      else if (e.getSource()==dMenuEstTerm)
      {
         splitPane.setDividerLocation(0.5);
      }
      
//////////////////////////Help menu button pressed      
      else if (e.getSource()==hMenuUserMan)
      {
         //create a new frame containing user manual in web page format (html)
         JFrame frame = new JFrame("User Manual");
         try
         {
            JEditorPane jp = new JEditorPane("file:doc/User Manual.html");
            jp.setEditable(false);
            JScrollPane scroll = new JScrollPane(jp);
            frame.getContentPane().add(scroll, "Center");
         }
         catch(IOException nfe)
         {
         }
         frame.setSize(500,600);
         frame.show();
      }
   }
   
   /**
    * Method to initiate the appropriate response to a
    * treeselectionevent (node selected on JTree).
    * @param e the treeselectionevent fired.
    */
   public void valueChanged(TreeSelectionEvent e)
   {
      DefaultMutableTreeNode node = (DefaultMutableTreeNode)tree.getLastSelectedPathComponent();
      //if statement necessary to dehighlight last selected
      //node when topology diagram used for viewing stats.
      if (node!=null)
      {
         treeChoice = String.valueOf(tree.getLastSelectedPathComponent());
         statsConverse.setText("Statistics for " + treeChoice);
         
         //the following string helps differentiate
         //between storage and computing elements
         String leafType = treeChoice.substring(0,2);
         
         //tell all threads which node is being viewed
         griddt.currView(treeChoice);
         for (int i = 0;i<noOfSitesWithCompOrStor;i++)
         {
            sitedt[i].currView(treeChoice);
            sedt[i].currView(treeChoice);
         }
         for (int i = 0;i<totalNoOfCEs;i++)
         {
            cedt[i].currView(treeChoice);
         }
         
         //produce the appropriate statistics pane
         //based on which node was selected.
         if (node.isRoot())
         {
            //construct "front" tab (tab 0).
            establishGridPane();
            establishGridSummaryTable();
            
            //tell change listener which tabs to listen for.
            gridListen = true;
            siteListen = false;
            compListen = false;
            storListen = false;
            
            //interrupt grid thread thus
            //causing it to put latest stats up immediately
            gridTabSelected = 0;
            griddt.interrupt();
         }
         else if (node.isLeaf() && leafType.equals("CE"))
         {
            //construct "front" tab (tab 0).
            this.establishCompPane();
            this.establishCompSummaryTable();
            
            //tell change listener which tabs to listen for.
            gridListen = false;
            siteListen = false;
            compListen = true;
            storListen = false;
            
            //find comp element and interrupt it's thread thus
            //causing it to put latest stats up immediately
            int index = 0;
            for (int i=0; i<totalNoOfCEs; i++)
            {
               String nodeCheck = cedt[i].getElement();
               if (nodeCheck.equals(treeChoice))
               {
                  index = i;
                  i = totalNoOfCEs;
               }
            }
            compTabSelected = 0;
            cedt[index].interrupt();
         }
         else if (node.isLeaf() && leafType.equals("SE"))
         {
            //construct "front" tab (tab 0).
            this.establishStorPane();
            this.establishStorSummaryTable();
            
            //tell change listener which tabs to listen for.
            gridListen = false;
            siteListen = false;
            compListen = false;
            storListen = true;
            
            //find stor element and interrupt it's thread thus
            //causing it to put latest stats up immediately
            int index = 0;
            for (int i=0; i<noOfSitesWithCompOrStor; i++)
            {
               String nodeCheck = sedt[i].getElement();
               if (nodeCheck.equals(treeChoice))
               {
                  index = i;
                  i = noOfSitesWithCompOrStor;
               }
            }
            storTabSelected = 0;
            sedt[index].interrupt();
         }
         else
         {
            //must have been site node selected
            siteSelected(treeChoice);
         }
      }
   }
   
   
   /**
    * Method to change statistics pane to hold site stats structure.
    * This has to be outwith treelistener as user could opt to view
    * site stats from topology diagram.
    * @param site site whose stats shall be displayed on stats pane.
    */
   public void siteSelected(String site)
   {
      //construct "front" tab (tab 0).
      this.establishSitePane();
      this.establishSiteSummaryTable();
      
      //tell change listener which tabs to listen for.
      gridListen = false;
      siteListen = true;
      compListen = false;
      storListen = false;
      
      //find site element and interrupt it's thread thus
      //causing it to put latest stats up immediately
      int index = 0;
      for (int i=0; i<noOfSitesWithCompOrStor; i++)
      {
         String nodeCheck = sitedt[i].getElement();
         if (nodeCheck.equals(treeChoice))
         {
            index = i;
            i = noOfSitesWithCompOrStor;
         }
      }
      siteTabSelected = 0;
      sitedt[index].interrupt();
   }
   
   
   /**
    * Method to initiate an appropriate response to a
    * changeEvent (different tab being selected on stats panel).
    * @param e the changeevent fired.
    */
   public void stateChanged(ChangeEvent e)
   {
      //get the index of the tab selected
      int tabSelected = ((JTabbedPane) e.getSource()).getSelectedIndex();
      //depending on which node selected search,
      //find and interrupt the appropriate thread
      if (gridListen)
      {
         //in here only if grid node selected
         gridTabSelected = tabSelected;
         griddt.interrupt();
      }
      if (siteListen)
      {
         //in here only if a site node selected
         siteTabSelected = tabSelected;
         int index = 0;
         for (int i=0; i<noOfSitesWithCompOrStor; i++)
         {
            String nodeCheck = sitedt[i].getElement();
            if (nodeCheck.equals(treeChoice))
            {
               index = i;
               i = noOfSitesWithCompOrStor;
            }
         }
         sitedt[index].interrupt();
      }
      if (compListen)
      {
         //in here only if a CE node selected
         compTabSelected = tabSelected;
         int index = 0;
         for (int i=0; i<totalNoOfCEs; i++)
         {
            String nodeCheck = cedt[i].getElement();
            if (nodeCheck.equals(treeChoice))
            {
               index = i;
               i = totalNoOfCEs;
            }
         }
         cedt[index].interrupt();
      }
      if (storListen)
      {
         //in here only if a SE node selected
         storTabSelected = tabSelected;
         int index = 0;
         for (int i=0; i<noOfSitesWithCompOrStor; i++)
         {
            String nodeCheck = sedt[i].getElement();
            if (nodeCheck.equals(treeChoice))
            {
               index = i;
               i = noOfSitesWithCompOrStor;
            }
         }
         sedt[index].interrupt();
      }
   }
   
   /**
    * Method to establish the structure of the statistics pane
    * appropriate for the display of grid stats.
    */
   private void establishGridPane()
   {
      if (statsPanel.getComponentCount()>2)
         statsPanel.remove(2);
      
      gridTabbedPane = new JTabbedPane();
      gridTabbedPane.addChangeListener(this);
      gridTabbedPane.setBackground(new Color(0xcc6699));
      
      gridTabbedPane.addTab("Summary Table",null, chartPanelg0);
      gridTabbedPane.setMnemonicAt(0, KeyEvent.VK_T);
      
      gridTabbedPane.addTab("Mean Job Time v Time",null, chartPanelg1);
      gridTabbedPane.setMnemonicAt(1, KeyEvent.VK_M);
      
      gridTabbedPane.addTab("Job Time Histogram",null, chartPanelg2);
      gridTabbedPane.setMnemonicAt(2, KeyEvent.VK_J);
      
      gridTabbedPane.addTab("No. of Replications v Time",null, chartPanelg3);
      gridTabbedPane.setMnemonicAt(3, KeyEvent.VK_N);
      
      gridTabbedPane.addTab("Remote File Accesses v Time",null, chartPanelg4);
      gridTabbedPane.setMnemonicAt(4, KeyEvent.VK_R);
      
      gridTabbedPane.addTab("Local File Accesses v Time",null, chartPanelg5);
      gridTabbedPane.setMnemonicAt(5, KeyEvent.VK_L);
      
      gridTabbedPane.addTab("Effective Network Usage",null, chartPanelg6);
      gridTabbedPane.setMnemonicAt(6, KeyEvent.VK_E);
      
      statsPanel.add(gridTabbedPane,"Center");
      statsPanel.repaint();
      statsPanel.updateUI();
   }
   
   /**
    * Method to establish the structure of the statistics pane
    * appropriate for the display of site stats.
    */
   private void establishSitePane()
   {
      if (statsPanel.getComponentCount()>2)
         statsPanel.remove(2);
      
      siteTabbedPane = new JTabbedPane();
      siteTabbedPane.addChangeListener(this);
      siteTabbedPane.setBackground(new Color(0xcc6699));
      
      siteTabbedPane.addTab("Summary Table",null, chartPanels0);
      siteTabbedPane.setMnemonicAt(0, KeyEvent.VK_T);
      
      siteTabbedPane.addTab("Mean Job Time v Time",null, chartPanels1);
      siteTabbedPane.setMnemonicAt(1, KeyEvent.VK_M);
      
      siteTabbedPane.addTab("Job Time Histogram",null, chartPanels2);
      siteTabbedPane.setMnemonicAt(2, KeyEvent.VK_J);
      
      siteTabbedPane.addTab("CE Usage v Time",null, chartPanels3);
      siteTabbedPane.setMnemonicAt(3, KeyEvent.VK_C);
      
      siteTabbedPane.addTab("SE Usage v Time",null, chartPanels4);
      siteTabbedPane.setMnemonicAt(4, KeyEvent.VK_S);
      
      statsPanel.add(siteTabbedPane,"Center");
      statsPanel.repaint();
      statsPanel.updateUI();
   }
   
   /**
    * Method to establish the structure of the statistics pane
    * appropriate for the display of computing element stats.
    */
   private void establishCompPane()
   {
      if (statsPanel.getComponentCount()>2)
         statsPanel.remove(2);
      
      compTabbedPane = new JTabbedPane();
      compTabbedPane.addChangeListener(this);
      compTabbedPane.setBackground(new Color(0xcc6699));
      
      compTabbedPane.addTab("Summary Table",null, chartPanelc0);
      compTabbedPane.setMnemonicAt(0, KeyEvent.VK_T);
      
      compTabbedPane.addTab("Mean Job Time v Time",null, chartPanelc1);
      compTabbedPane.setMnemonicAt(1, KeyEvent.VK_M);
      
      compTabbedPane.addTab("Job Time Histogram",null, chartPanelc2);
      compTabbedPane.setMnemonicAt(2, KeyEvent.VK_J);
      
      compTabbedPane.addTab("CE Usage v Time",null, chartPanelc3);
      compTabbedPane.setMnemonicAt(3, KeyEvent.VK_C);
      
      compTabbedPane.addTab("Remote File Accesses v Time",null, chartPanelc4);
      compTabbedPane.setMnemonicAt(4, KeyEvent.VK_R);
      
      compTabbedPane.addTab("Local File Accesses v Time",null, chartPanelc5);
      compTabbedPane.setMnemonicAt(5, KeyEvent.VK_L);
      
      statsPanel.add(compTabbedPane,"Center");
      statsPanel.repaint();
      statsPanel.updateUI();
   }
   
   /**
    * Method to establish the structure of the statistics pane
    * appropriate for the display of storage element stats.
    */
   private void establishStorPane()
   {
      if (statsPanel.getComponentCount()>2)
         statsPanel.remove(2);
      
      storTabbedPane = new JTabbedPane();
      storTabbedPane.addChangeListener(this);
      storTabbedPane.setBackground(new Color(0xcc6699));
      
      storTabbedPane.addTab("Summary Table",null, chartPanelst0);
      storTabbedPane.setMnemonicAt(0, KeyEvent.VK_T);
      
      storTabbedPane.addTab("SE Usage V Time",null, chartPanelst1);
      storTabbedPane.setMnemonicAt(1, KeyEvent.VK_U);
      
      storTabbedPane.addTab("SE Usage Pie",null, chartPanelst2);
      storTabbedPane.setMnemonicAt(2, KeyEvent.VK_P);
      
      statsPanel.add(storTabbedPane,"Center");
      statsPanel.repaint();
      statsPanel.updateUI();
   }
   
   /**
    * Method to establish the structure of the table on
    * stats tab 0 appropriate for the display of grid stats.
    */
   static private void establishGridSummaryTable()
   {
      if (chartPanelg0.getComponentCount()>0)
         chartPanelg0.remove(0);
      String[] columnNames =
      {"Parameter", "Value"};
      Object[][] data =
      {
         {"Number of Jobs Remaining",""},
         {"Mean Job Time of all Jobs on Grid",""},
         {"Total Number of Replications",""},
         {"Total Number of Local File Accesses",""},
         {"Total Number of Remote File Accesses",""},
         {"Percentage of CEs in Use", ""},
         {"Percentage of Storage Filled/Available",""},
         {"Effective Network Usage",new Float(0.0)},
      };
      gridTable = new JTable(data, columnNames);
      JScrollPane gridTableScroll = new JScrollPane(gridTable);
      chartPanelg0.add(gridTableScroll);
   }
   
   /**
    * Method to add statistical content to the table on
    * stats tab 0 of grid stats.
    * @param grid String format of the node (grid),
    * whose stats table, this method shall update.
    */
   static public void addGridSummaryTableValues(String grid)
   {
      if ((grid.equals(treeChoice)&&gridTabSelected == 0)||printingAll)
      {
         //initialise variables
         int totalNoOfJobs=0;
         float totalJobTime=0;
         int totalRemoteReads=0;
         int totalLocalReads=0;
         int totalActiveCEs=0;
         float percFilled=0;
         float totalCapacity=0;
         float totalSpareCapacity=0;
         //cycle through all CEs and SEs and sum data
         for( Enumeration eSite = gc.allGridSites(); eSite.hasMoreElements() ;)
         {
            GridSite gs = (GridSite) eSite.nextElement();
            if(gs.hasCE() || gs.hasSEs())
            {
               if (gs.hasCE())
               {
                  ComputingElement ce = gs.getCE();
                  Statistics st = ce.getStatistics();
                  
                  Object numberOfJobsObj = st.getStatistic("numberOfJobs");
                  String numberOfJobsStr = numberOfJobsObj.toString();
                  totalNoOfJobs += Integer.parseInt(numberOfJobsStr);
                  
                  Object totalJobTimeObj = st.getStatistic("totalJobTime");
                  String totalJobTimeStr = totalJobTimeObj.toString();
                  totalJobTime += Float.parseFloat(totalJobTimeStr);
                  
                  Object remoteReadsObj = st.getStatistic("remoteReads");
                  String remoteReadsStr = remoteReadsObj.toString();
                  totalRemoteReads += Integer.parseInt(remoteReadsStr);
                  
                  Object localReadsObj = st.getStatistic("localReads");
                  String localReadsStr = localReadsObj.toString();
                  totalLocalReads += Integer.parseInt(localReadsStr);
                  
                  Object o = st.getStatistic("status");
                  String s = o.toString();
                  Boolean B =  new Boolean(s);
                  boolean b = B.booleanValue();
                  if (b)
                     totalActiveCEs++;
               }
               if (gs.hasSEs())
               {
                  StorageElement se = gs.getSE();
                  totalCapacity += se.getCapacity();
                  totalSpareCapacity += se.getAvailableSpace();
               }
               percFilled = (1-totalSpareCapacity/totalCapacity)*100;
            }
         }
         
         //assign appropriate values to string variables
         Statistics gst = gc.getStatistics();
         Object replicationsObj = gst.getStatistic("replications");
         String replicationsStr = replicationsObj.toString();
         float replicas = Float.parseFloat(replicationsStr);
         float ENU = (totalRemoteReads + replicas)/(float)(totalLocalReads+totalRemoteReads);
         Float ENUFlo = new Float(ENU);
         int noJobsRem = params.getNoOfJobs() - totalNoOfJobs;
         String noJobsRemStr = String.valueOf(noJobsRem);
         String meanJobTimeStr = String.valueOf((int)(totalJobTime/totalNoOfJobs));
         String remoteReadsStr = String.valueOf(totalRemoteReads);
         String localReadsStr = String.valueOf(totalLocalReads);
         String totalActiveCEsStr = String.valueOf(100*totalActiveCEs/gc.countCEs())+"%";
         String percFilledStr = String.valueOf(percFilled)+"%";
         
         /*if (not printingAll)
          *    update JTable on tab 0
          *else if (printingAll)
          *    append xhtml text
          */
         if (!printingAll)
         {
            gridTable.setValueAt(noJobsRemStr,0,1);
            gridTable.setValueAt(meanJobTimeStr,1,1);
            gridTable.setValueAt(replicationsStr,2,1);
            gridTable.setValueAt(localReadsStr,3,1);
            gridTable.setValueAt(remoteReadsStr,4,1);
            gridTable.setValueAt(totalActiveCEsStr,5,1);
            gridTable.setValueAt(percFilledStr,6,1);
            gridTable.setValueAt(ENUFlo,7,1);
            //refresh tab 0 with latest stats
            chartPanelg0.repaint();
            chartPanelg0.updateUI();
            //set JPEG name for use when saving charts in real time
            printableTitle = "Summary Table "+treeChoice;
         }
         if (printingAll)
         {
            //append xhtml text which will
            //constuct and fill with site stats an xhtml table
            overallSummaryText += ("<table border=\"1\">"+
            "<tr><th>Grid Summary Table</th><th></th></tr>"+
            "<tr><td>Number of Jobs Remaining</td><td>"+noJobsRemStr+"</td></tr>"+
            "<tr><td>Mean Job Time of all Jobs on Grid</td><td>"+meanJobTimeStr+"</td></tr>"+
            "<tr><td>Total Number of Replications</td><td>"+replicationsStr+"</td></tr>"+
            "<tr><td>Total Number of Local File Accesses</td><td>"+localReadsStr+"</td></tr>"+
            "<tr><td>Total Number of Remote File Accesses</td><td>"+remoteReadsStr+"</td></tr>"+
            "<tr><td>Percentage of CEs in Use</td><td>"+totalActiveCEsStr+"</td></tr>"+
            "<tr><td>Percentage of Storage Filled/Available</td><td>"+percFilledStr+"</td></tr>"+
            "<tr><td>Effective Network Usage</td><td>"+ENUFlo+"</td></tr></table>\n<p>\n");
         }
      }
   }
   
   /**
    * Method to establish the structure of the table on
    * stats tab 0 appropriate for the display of site stats.
    */
   private void establishSiteSummaryTable()
   {
      if (chartPanels0.getComponentCount()>0)
         chartPanels0.remove(0);
      String[] columnNames =
      {"Parameter", "Value"};
      Object[][] data =
      {
         {"Number of CEs",""},
         {"Size of SE",""},
         {"CE Status (active/idle)",""},
         {"SE Status (percentage filled)",""},
         {"Number of Jobs Processed",""},
         {"Number of Jobs in Queue", ""},
         {"Mean Job Time",""},
         {"Number of Local File Accesses",""},
         {"Number of Remote File Accesses",""},
         
      };
      siteTable = new JTable(data, columnNames);
      JScrollPane siteTableScroll = new JScrollPane(siteTable);
      chartPanels0.add(siteTableScroll);
   }
   
   /**
    * Method to add statistical content to the table on
    * stats tab 0 of site stats.
    * @param sitEle String format of the node (site),
    * whose stats table, this method shall update.
    */
   static public void addSiteSummaryTableValues(String sitEle)
   {
      if ((sitEle.equals(treeChoice)&&siteTabSelected == 0)||printingAll)
      {
         //get the relevant gridsite object
         int ind = sitEle.indexOf('e');
         String siteno = sitEle.substring(ind+1);
         int siteID = Integer.parseInt(siteno);
         site = gc.findGridSiteByID(siteID);
         /*if (not printingAll)
          *    update JTable on tab 0 with latest stats
          *else if (printingAll)
          *    append xhtml text
          */
         if(!printingAll)
         {
            //get the number of CEs at this site
            int noOfCEs = site.countCEs();
            /*if (site has CEs)
             *    update JTable on tab 0 with latest stats from CE
             *else
             *    update JTable with N/A "values"
             */
            if (noOfCEs>0)
            {
               String noCEs = String.valueOf(noOfCEs);
               siteTable.setValueAt(noCEs,0,1);
               //obtain the statistics object for comEle at this site
               ComputingElement ce = site.getCE();
               Statistics st = ce.getStatistics();
               
               Object o = st.getStatistic("status");
               String s = o.toString();
               Boolean B =  new Boolean(s);
               boolean b = B.booleanValue();
               if (b)
                  siteTable.setValueAt("active",2,1);
               else
                  siteTable.setValueAt("idle",2,1);
               siteTable.setValueAt(st.getStatistic("numberOfJobs"),4,1);
               siteTable.setValueAt(st.getStatistic("queueLength"),5,1);
               siteTable.setValueAt(st.getStatistic("meanJobTime"),6,1);
               siteTable.setValueAt(st.getStatistic("localReads"),7,1);
               siteTable.setValueAt(st.getStatistic("remoteReads"),8,1);
            }
            else
            {
               //in here if site has no CE
               siteTable.setValueAt("0",0,1);
               siteTable.setValueAt("N/A",2,1);
               siteTable.setValueAt("N/A",4,1);
               siteTable.setValueAt("N/A",5,1);
               siteTable.setValueAt("N/A",6,1);
               siteTable.setValueAt("N/A",7,1);
               siteTable.setValueAt("N/A",8,1);
            }
            
            //get the number of SEs at this site
            int noOfSEs = site.countSEs();
            /*if (site has SEs)
             *    update JTable on tab 0 with latest stats from SE
             *else
             *    update JTable with N/A "values"
             */
            if (noOfSEs>0)
            {
               StorageElement se = site.getSE();
               String capacity = se.getCapacity()/1000+"GB";
               siteTable.setValueAt(capacity,1,1);
               String percFilled = (1-(float)se.getAvailableSpace()/
               (float)se.getCapacity())*100+"%";
               siteTable.setValueAt(percFilled,3,1);
            }
            else
            {
               //in here if site has no SE
               siteTable.setValueAt("N/A",1,1);
               siteTable.setValueAt("N/A",3,1);
            }
            
            //refresh tab 0 with latest stats
            chartPanels0.repaint();
            chartPanels0.updateUI();
            //set JPEG name for use when saving charts in real time
            printableTitle = "Summary Table "+treeChoice;
         }
         
         else if (printingAll)
         {
            //obtain CE stats (if a CE exists) and assign appropriate values as strings
            String activity = "N/A";
            String numberOfJobs = "N/A";
            String queueLength = "N/A";
            String meanJobTime = "N/A";
            String localReads = "N/A";
            String remoteReads = "N/A";
            int noOfCEs = site.countCEs();
            String noCEs = String.valueOf(noOfCEs);
            if (noOfCEs>0)
            {
               ComputingElement ce = site.getCE();
               Statistics st = ce.getStatistics();
               Object o = st.getStatistic("status");
               String s = o.toString();
               Boolean B =  new Boolean(s);
               boolean b = B.booleanValue();
               if (b)
                  activity = "active";
               else
                  activity = "idle";
               numberOfJobs = String.valueOf(st.getStatistic("numberOfJobs"));
               queueLength = String.valueOf(st.getStatistic("queueLength"));
               meanJobTime = String.valueOf(st.getStatistic("meanJobTime"));
               localReads = String.valueOf(st.getStatistic("localReads"));
               remoteReads = String.valueOf(st.getStatistic("remoteReads"));
            }
            
            //obtain SE stats (if a SE exists) and assign appropriate values as strings
            String capacity = "N/A";
            String percFilled = "N/A";
            int noOfSEs = site.countSEs();
            if (noOfSEs>0)
            {
               StorageElement se = site.getSE();
               capacity = se.getCapacity()/1000+"GB";
               percFilled = (1-(float)se.getAvailableSpace()/
               (float)se.getCapacity())*100+"%";
            }
            //append xhtml text which will
            //constuct and fill with site stats an xhtml table
            overallSummaryText += ("<h2>"+sitEle+"</h2>\n");
            overallSummaryText += ("<table border=\"1\">"+
            "<tr><th>"+sitEle+" Summary Table</th><th></th></tr>"+
            "<tr><td>Number of CE's</td><td>"+noCEs+"</td></tr>"+
            "<tr><td>Size of SE</td><td>"+capacity+"</td></tr>"+
            "<tr><td>CE Status (active/idle)</td><td>"+activity+"</td></tr>"+
            "<tr><td>SE Status (percentage filled)</td><td>"+percFilled+"</td></tr>"+
            "<tr><td>Number of Jobs Processed</td><td>"+numberOfJobs+"</td></tr>"+
            "<tr><td>Number of Jobs in Queue</td><td>"+queueLength+"</td></tr>"+
            "<tr><td>Mean Job Time</td><td>"+meanJobTime+"</td></tr>"+
            "<tr><td>Number of Local File Accesses</td><td>"+localReads+"</td></tr>"+
            "<tr><td>Number of Remote File Accesses</td><td>"+remoteReads+"</td></tr></table>\n<p>\n");
         }
      }
   }
   
   /**
    * Method to establish the structure of the table on stats
    * tab 0 appropriate for the display of computing element stats.
    */
   private void establishCompSummaryTable()
   {
      if (chartPanelc0.getComponentCount()>0)
         chartPanelc0.remove(0);
      String[] columnNames =
      {"Parameter", "Value"};
      Object[][] data =
      {
         {"Number of Worker Nodes",""},
         {"CE Status (active/idle)",""},
         {"Number of Jobs Processed",""},
         {"Number of Jobs in Queue", ""},
         {"Mean Job Time",""},
         {"Number of Local File Accesses",""},
         {"Number of Remote File Accesses",""},
         {"CE Usage (time active/total time running)",""},
         {"CE ",""},
      };
      compTable = new JTable(data, columnNames);
      JScrollPane compTableScroll = new JScrollPane(compTable);
      chartPanelc0.add(compTableScroll);
   }
   
   /**
    * Method to add statistical content to the table on
    * stats tab 0 of computing element stats.
    * @param comEle String format of the node (computing element),
    * whose stats table, this method shall update.
    */
   static public void addCompSummaryTableValues(String comEle)
   {
      if ((comEle.equals(treeChoice)&&compTabSelected == 0)||printingAll)
      {
         //get the relevant gridsite object
         int ind = comEle.indexOf('e');
         String siteno = comEle.substring(ind+1);
         int siteID = Integer.parseInt(siteno);
         site = gc.findGridSiteByID(siteID);
         //obtain the statistics object for comEle
         ComputingElement ce = site.getCE();
         Statistics st = ce.getStatistics();
         
         Object o = st.getStatistic("status");
         String s = o.toString();
         Boolean B =  new Boolean(s);
         boolean b = B.booleanValue();
         
         /*if (not printingAll)
          *    update JTable on tab 0 with latest stats
          *else if (printingAll)
          *    append xhtml text
          */
         if(!printingAll)
         {
            if (b)
               compTable.setValueAt("active",1,1);
            else
               compTable.setValueAt("idle",1,1);
            compTable.setValueAt(st.getStatistic("workerNodes"),0,1);
            compTable.setValueAt(st.getStatistic("numberOfJobs"),2,1);
            compTable.setValueAt(st.getStatistic("queueLength"),3,1);
            compTable.setValueAt(st.getStatistic("meanJobTime"),4,1);
            compTable.setValueAt(st.getStatistic("localReads"),5,1);
            compTable.setValueAt(st.getStatistic("remoteReads"),6,1);
            compTable.setValueAt(st.getStatistic("usage"),7,1);
            compTable.setValueAt(comEle,8,1);
            //refresh tab 0 with latest stats
            chartPanelc0.repaint();
            chartPanelc0.updateUI();
            //set JPEG name for use when saving charts in real time
            printableTitle = "Summary Table "+treeChoice;
         }
         
         else if (printingAll)
         {
            //append xhtml text which will
            //construct (and fill with latest stats) an xhtml table
            String activeOrIdle = "";
            if (b)
               activeOrIdle = "active";
            else
               activeOrIdle = "idle";
            overallSummaryText += ("<h2>"+ce+"</h2>\n");
            overallSummaryText += ("<table border=\"1\">"+
            "<tr><th>"+comEle+" Summary Table</th><th></th></tr>"+
            "<tr><td>Number of Worker Node's</td><td>"+st.getStatistic("workerNodes")+"</td></tr>"+
            "<tr><td>CE Status (active/idle)</td><td>"+activeOrIdle+"</td></tr>"+
            "<tr><td>Number of Jobs Processed</td><td>"+st.getStatistic("numberOfJobs")+"</td></tr>"+
            "<tr><td>Number of Jobs in Queue</td><td>"+st.getStatistic("queueLength")+"</td></tr>"+
            "<tr><td>Mean Job Time</td><td>"+st.getStatistic("meanJobTime")+"</td></tr>"+
            "<tr><td>Number of Local File Accesses</td><td>"+st.getStatistic("localReads")+"</td></tr>"+
            "<tr><td>Number of Remote File Accesses</td><td>"+st.getStatistic("remoteReads")+"</td></tr>"+
            "<tr><td>CE Usage (time active/total time running)</td><td>"+st.getStatistic("usage")+"</td></tr></table>\n<p>\n");
         }
      }
   }
   
   /**
    * Method to establish the structure of the table on stats
    * tab 0 appropriate for the display of storage element stats.
    */
   private void establishStorSummaryTable()
   {
      if (chartPanelst0.getComponentCount()>0)
         chartPanelst0.remove(0);
      String[] columnNames =
      {"Parameter", "Value"};
      Object[][] data =
      {
         {"Size of this SE",""},
         {"SE Status (percentage filled)",""},
         {"Number of Files Stored at this SE",""},
      };
      storTable = new JTable(data, columnNames);
      JScrollPane storTableScroll = new JScrollPane(storTable);
      chartPanelst0.add(storTableScroll);
   }
   
   /**
    * Method to add statistical content to the table on
    * stats tab 0 of storage element stats.
    * @param stoEle String format of the node (storage element),
    * whose stats table, this method shall update.
    */
   static public void addStorSummaryTableValues(String stoEle)
   {
      if ((stoEle.equals(treeChoice)&&storTabSelected == 0)||printingAll)
      {
         //get the relevant gridsite object
         int ind = stoEle.indexOf('e');
         String siteno = stoEle.substring(ind+1);
         int siteID = Integer.parseInt(siteno);
         site = gc.findGridSiteByID(siteID);
         
         StorageElement se = site.getSE();
         //obtain the statistics (capacity, percFilled, noStoredFiles) for stoEle
         String capacity = se.getCapacity()/1000+"GB";
         String percFilled = (1-(float)se.getAvailableSpace()/
         (float)se.getCapacity())*100+"%";
         int stoFiles = se.numberOfStoredFiles();
         String noStoredFiles = String.valueOf(stoFiles);
         
         /*if (not printingAll)
          *    update JTable on tab 0 with latest stats
          *else if (printingAll)
          *    append xhtml text
          */
         if (!printingAll)
         {
            storTable.setValueAt(capacity,0,1);
            storTable.setValueAt(percFilled,1,1);
            storTable.setValueAt(noStoredFiles,2,1);
            //refresh tab 0 with latest stats
            chartPanelst0.repaint();
            chartPanelst0.updateUI();
            //set JPEG name for use when saving charts in real time
            printableTitle = "Summary Table "+treeChoice;
         }
         
         else if (printingAll)
         {
            //append xhtml text which will
            //construct (and fill with latest stats) an xhtml table
            overallSummaryText += ("<h2>"+se+"</h2>\n");
            overallSummaryText += ("<table border=\"1\">"+
            "<tr><th>"+stoEle+" Summary Table</th><th></th></tr>"+
            "<tr><td>Size of SE</td><td>"+capacity+"</td></tr>"+
            "<tr><td>SE Status (percentage filled)</td><td>"+percFilled+"</td></tr>"+
            "<tr><td>Number of Files Stored</td><td>"+noStoredFiles+"</td></tr></table>\n<p>\n");
         }
      }
   }
   
   /**
    * Method to update the grid mean job time graph.
    * @param seriesGridMeanJobVTime Series of domain and range values
    * (mean job time against time) for whole grid.
    * @param grid String format of the node (grid), whose graph, this method shall update.
    */
   public static void addGridMeanJobGraph(XYSeries seriesGridMeanJobVTime, String grid)
   {
      if ((grid.equals(treeChoice)&&gridTabSelected==1)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesGridMeanJobVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart gridMJChart = ChartFactory.createXYAreaChart
         (grid +" Mean Job Time V Time ",            // Title
         "Time (sec)",                               // X-Axis label
         "Grid Mean Job Time (ms)",                  // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Grid MJT V Time";
         printableChart = gridMJChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelg1.getComponentCount()>0)
               chartPanelg1.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = gridMJChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelg1.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelg1.repaint();
            chartPanelg1.updateUI();
         }
      }
   }
   
   /**
    * Method to update the grid job time histogram graph.
    * @param histarray Array of job time values for whole grid.
    * @param grid String format of the node (grid), whose graph, this method shall update.
    */
   public static void addGridJobHistGraph(double [] histarray, String grid)
   {
      
      if ((grid.equals(treeChoice)&&gridTabSelected==2)||printingAll)
      {
         //Establishes histogram dataset for use by chart
         HistogramDataset hist = new HistogramDataset();
         hist.addSeries("histseries",histarray,numOfGridBars);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart gridJTHChart = ChartFactory.createHistogram
         ("Job Time Histogram for " + grid,          // Title
         "Job Time Bins ("+numOfGridBars+")",        // X-Axis label
         "Number",                                   // Y-Axis label
         hist,                                       // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Grid JTHist V Time";
         printableChart = gridJTHChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelg2.getComponentCount()>0)
               chartPanelg2.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = gridJTHChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelg2.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelg2.repaint();
            chartPanelg2.updateUI();
         }
      }
   }
   
   /**
    * Method to update the grid number of replications graph.
    * @param seriesGridNoRepsVTime Series of domain and range values
    * (number of replications against time) for whole grid.
    * @param grid String format of the node (grid), whose graph, this method shall update.
    */
   public static void addGridNoRepGraph(XYSeries seriesGridNoRepsVTime, String grid)
   {
      if ((grid.equals(treeChoice)&&gridTabSelected==3)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesGridNoRepsVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart gridNRVTChart = ChartFactory.createXYAreaChart
         (grid +" No. Replications V Time ",         // Title
         "Time (sec)",                               // X-Axis label
         "No. Replications",                         // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Grid No. Reps V Time";
         printableChart = gridNRVTChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelg3.getComponentCount()>0)
               chartPanelg3.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = gridNRVTChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelg3.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelg3.repaint();
            chartPanelg3.updateUI();
         }
      }
   }
   
   /**
    * Method to update the grid remote file accesses graph.
    * @param seriesGridRemFilAccVTime Series of domain and range values
    * (remote file accesses against time) for whole grid.
    * @param grid String format of the node (grid), whose graph, this method shall update.
    */
   public static void addGridRemFilAccGraph(XYSeries seriesGridRemFilAccVTime, String grid)
   {
      if ((grid.equals(treeChoice)&&gridTabSelected==4)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesGridRemFilAccVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart gridRFAChart = ChartFactory.createXYAreaChart
         (grid +" Remote File Accesses V Time ",     // Title
         "Time (sec)",                               // X-Axis label
         "Remote File Accesses",                     // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Grid RFA V Time";
         printableChart = gridRFAChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelg4.getComponentCount()>0)
               chartPanelg4.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = gridRFAChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelg4.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelg4.repaint();
            chartPanelg4.updateUI();
         }
      }
   }
   
   /**
    * Method to update the grid local file accesses usage graph.
    * @param seriesGridLocFilAccVTime Series of domain and range values
    * (local file accesses against time) for whole grid.
    * @param grid String format of the node (grid), whose graph, this method shall update.
    */
   public static void addGridLocFilAccGraph(XYSeries seriesGridLocFilAccVTime, String grid)
   {
      if ((grid.equals(treeChoice)&&gridTabSelected==5)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesGridLocFilAccVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart gridLFAChart = ChartFactory.createXYAreaChart
         (grid +" Local File Accesses V Time ",      // Title
         "Time (sec)",                               // X-Axis label
         "Local File Accesses",                      // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Grid LFA V Time";
         printableChart = gridLFAChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            
            
            //Place updated chart on chartPanel,
            BufferedImage im = gridLFAChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            if (chartPanelg5.getComponentCount()>0)
               chartPanelg5.remove(0);
            chartPanelg5.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelg5.repaint();
            chartPanelg5.updateUI();
         }
      }
   }
   
   /**
    * Method to update the grid effective network usage graph.
    * @param seriesGridENUVTime Series of domain and range values
    * (effective network usage against time) for whole grid.
    * @param grid String format of the node (grid), whose graph, this method shall update.
    */
   public static void addGridENUGraph(XYSeries seriesGridENUVTime, String grid)
   {
      /* The if statement which envelopes the code within this method
       * checks to see if user is wishing to view this chart
       * by confirming the appropriate tree node and appropriate stats
       * tab have been selected, or if user is printing final overall summary.
       * These are the only two conditions under which the program need
       * execute the code within ensuing if loop.
       */
      if ((grid.equals(treeChoice)&&gridTabSelected==6)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesGridENUVTime);
  /*       for(int i=0; i<seriesGridENUVTime.getItemCount();i++) {
          System.out.println(seriesGridENUVTime.getYValue(i));
         }*/
             //instantiate a new chart with the fresh dataset
         JFreeChart gridRFAChart = ChartFactory.createXYAreaChart
         (grid +" Effective Network Usage V Time ",  // Title
         "Time (sec)",                               // X-Axis label
         "Effective Network Usage",                  // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // urls
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Grid ENU V Time";
         printableChart = gridRFAChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n</p>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelg6.getComponentCount()>0)
               chartPanelg6.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = gridRFAChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelg6.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelg6.repaint();
            chartPanelg6.updateUI();
         }
      }
   }
   
   
   /**
    * Method to update the site job time histogram graph.
    * @param histarray Array of job time values for whole site.
    * @param sit String format of the node (site), whose graph, this method shall update.
    */
   public static void addSiteJobHistGraph(double [] histarray, String sit)
   {
      if ((sit.equals(treeChoice)&&siteTabSelected==2)||printingAll)
      {
         //Establishes histogram dataset for use by chart
         HistogramDataset hist = new HistogramDataset();
         hist.addSeries("histseries",histarray,numOfSiteBars);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart sitJTHChart = ChartFactory.createHistogram
         ("Job Time Histogram for " +sit,            // Title
         "Job Time Bins("+numOfSiteBars+")",         // X-Axis label
         "Number",                                   // Y-Axis label
         hist,                                       // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Site JTHist V Time " + sit;
         printableChart = sitJTHChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanels2.getComponentCount()>0)
               chartPanels2.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = sitJTHChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanels2.add(statsScroll);
            
            //refresh chartPanel.
            chartPanels2.repaint();
            chartPanels2.updateUI();
         }
      }
   }
   
   /**
    * Method to update the site computing element usage graph.
    * @param seriesSitCEUseVTime Series of domain and range values
    * (computing element usage against time) for whole site.
    * @param sit String format of the node (site), whose graph, this method shall update.
    */
   public static void addSiteCEUseGraph(XYSeries seriesSitCEUseVTime, String sit)
   {
      if ((sit.equals(treeChoice)&&siteTabSelected==3)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesSitCEUseVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart sitCEUseChart = ChartFactory.createXYAreaChart
         ("Site CE Usage V Time "+sit,               // Title
         "Time (sec)",                               // X-Axis label
         "Site CE Usage (% of Time in Usage)",       // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Site CEU V Time " + sit;
         printableChart = sitCEUseChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanels3.getComponentCount()>0)
               chartPanels3.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = sitCEUseChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanels3.add(statsScroll);
            
            //refresh chartPanel.
            chartPanels3.repaint();
            chartPanels3.updateUI();
         }
      }
   }
   
   /**
    * Method to update the site storage element usage graph.
    * @param seriesSitSEUseVTime Series of domain and range values
    * (storage element usage against time) for whole site.
    * @param sit String format of the node (site), whose graph, this method shall update.
    */
   public static void addSiteSEUseGraph(XYSeries seriesSitSEUseVTime, String sit)
   {
      if ((sit.equals(treeChoice)&&siteTabSelected==4)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesSitSEUseVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart sitSEUseChart = ChartFactory.createXYAreaChart
         ("Site SE Usage V Time "+sit,               // Title
         "Time (sec)",                               // X-Axis label
         "Site SE Usage (GBytes)",                   // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Site SEU V Time " + sit;
         printableChart = sitSEUseChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n</p>");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanels4.getComponentCount()>0)
               chartPanels4.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = sitSEUseChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanels4.add(statsScroll);
            
            //refresh chartPanel.
            chartPanels4.repaint();
            chartPanels4.updateUI();
         }
      }
   }
   
   /**
    * Method to update the computing elements mean job time graph.
    * @param seriesMeanJobVTime Series of domain and range values
    * (mean job time against time) for computing element.
    * @param ce String format of the node (computing element), whose graph, this method shall update.
    */
   public static void addCompMeanJobGraph(XYSeries seriesMeanJobVTime, String ce)
   {
      if ((ce.equals(treeChoice)&&compTabSelected==1)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesMeanJobVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart meanJobChart = ChartFactory.createXYAreaChart
         ("Mean Job Time V Time "+ce,                // Title
         "Time (sec)",                               // X-Axis label
         "Mean Job Time (ms)",                       // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Comp MJT V Time " + ce;
         printableChart = meanJobChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelc1.getComponentCount()>0)
               chartPanelc1.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = meanJobChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelc1.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelc1.repaint();
            chartPanelc1.updateUI();
         }
      }
   }
   
   /**
    * Method to update the computing elements job time histogram graph.
    * @param histarray Array of job time values for computing element.
    * @param ce String format of the node (computing element), whose graph, this method shall update.
    */
   public static void addCompJobHistGraph(double [] histarray, String ce)
   {
      if ((ce.equals(treeChoice)&&compTabSelected==2)||printingAll)
      {
         //Establishes histogram dataset for use by chart
         HistogramDataset hist = new HistogramDataset();
         hist.addSeries("histseries",histarray,numOfCompBars);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart compJTHChart = ChartFactory.createHistogram
         ("Job Time Histogram for " +ce,             // Title
         "Job Time Bins ("+numOfCompBars+")",        // X-Axis label
         "Number",                                   // Y-Axis label
         hist,                                       // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Comp JTHist V Time " + ce;
         printableChart = compJTHChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelc2.getComponentCount()>0)
               chartPanelc2.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = compJTHChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelc2.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelc2.repaint();
            chartPanelc2.updateUI();
         }
      }
   }
   
   /**
    * Method to update the computing elements usage graph.
    * @param seriesCEUseVTime Series of domain and range values
    * (computing element usage against time) for computing element.
    * @param ce String format of the node (computing element), whose graph, this method shall update.
    */
   public static void addCompCEUseGraph(XYSeries seriesCEUseVTime, String ce)
   {
      if ((ce.equals(treeChoice)&&compTabSelected==3)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesCEUseVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart cEUseChart = ChartFactory.createXYAreaChart
         ("CE Usage V Time "+ce,                     // Title
         "Time (sec)",                               // X-Axis label
         "CE Usage (% of Time in Usage)",            // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Comp Usage V Time " + ce;
         printableChart = cEUseChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelc3.getComponentCount()>0)
               chartPanelc3.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = cEUseChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelc3.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelc3.repaint();
            chartPanelc3.updateUI();
         }
      }
   }
   
   /**
    * Method to update the computing elements remote file accesses graph.
    * @param seriesRemAccVTime Series of domain and range values
    * (remote file accesses against time) for computing element.
    * @param ce String format of the node (computing element), whose graph, this method shall update.
    */
   public static void addCompRemFilAccGraph(XYSeries seriesRemAccVTime, String ce)
   {
      if ((ce.equals(treeChoice)&&compTabSelected==4)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesRemAccVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart remFilchart = ChartFactory.createXYAreaChart
         ("Remote File Accesses V Time "+ce,         // Title
         "Time (sec)",                               // X-Axis label
         "Remote File Accesses",                     // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Comp RFA V Time " + ce;
         printableChart = remFilchart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"), printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelc4.getComponentCount()>0)
               chartPanelc4.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = remFilchart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelc4.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelc4.repaint();
            chartPanelc4.updateUI();
         }
      }
   }
   
   /**
    * Method to update the computing elements local file accesses graph.
    * @param seriesLocAccVTime Series of domain and range values
    * (local file accesses against time) for computing element.
    * @param ce String format of the node (computing element), whose graph, this method shall update.
    */
   public static void addCompLocFilAccGraph(XYSeries seriesLocAccVTime, String ce)
   {
      if ((ce.equals(treeChoice)&&compTabSelected==5)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesLocAccVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart locFilchart = ChartFactory.createXYAreaChart
         ("Local File Accesses V Time "+ce,          // Title
         "Time (sec)",                               // X-Axis label
         "Local File Accesses",                      // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Comp LFA V Time " + ce;
         printableChart = locFilchart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n</p>");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelc5.getComponentCount()>0)
               chartPanelc5.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = locFilchart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelc5.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelc5.repaint();
            chartPanelc5.updateUI();
         }
      }
   }
   
   /**
    * Method to update the storage elements SE usage graph.
    * @param seriesStUseVTime Series of domain and range values
    * (storage element usage against time) for storage element.
    * @param se String format of the node (storage element), whose graph, this method shall update.
    */
   public static void addStorUseGraph(XYSeries seriesStUseVTime, String se)
   {
      if ((se.equals(treeChoice)&&storTabSelected==1)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesStUseVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart sEUseChart = ChartFactory.createXYAreaChart
         ("SE Usage V Time "+se,                     // Title
         "Time (sec)",                               // X-Axis label
         "SE Usage (GBytes)",                        // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Stor Usage V Time " + se;
         printableChart = sEUseChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n</p>");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelst1.getComponentCount()>0)
               chartPanelst1.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = sEUseChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelst1.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelst1.repaint();
            chartPanelst1.updateUI();
         }
      }
   }
   
   /** 
    * Method to update the current storage usage pie chart.
    * @param pieDataset two current values - used storage and available storage.
    * @param sto SString format of the node (storage element), whose graph, this method shall update.
    */
   public static void addStorPieChart(DefaultPieDataset pieDataset, String sto)
   {
      if ((sto.equals(treeChoice)&&storTabSelected==2)||printingAll)
      {
         //instantiate a new pie chart with the fresh dataset
         JFreeChart piechart = ChartFactory.createPieChart
         ("Current Status of "+sto,                  // Title
         pieDataset,                                // Dataset
         false,                                     // legend
         false,                                     // tooltips
         false                                      // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "SE Usage (current) " + sto;
         printableChart = piechart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanelst2.getComponentCount()>0)
               chartPanelst2.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = piechart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanelst2.add(statsScroll);
            
            //refresh chartPanel.
            chartPanelst2.repaint();
            chartPanelst2.updateUI();
         }
      }
   }
   
   /**
    * Method to update the site mean job time graph.
    * @param seriesSitMeanJobVTime Series of domain and range values
    * (mean job time against time) for whole site.
    * @param sit String format of the node (site), whose graph, this method shall update.
    */
   public static void addSiteMeanJobGraph(XYSeries seriesSitMeanJobVTime, String sit)
   {
      if ((sit.equals(treeChoice)&&siteTabSelected==1)||printingAll)
      {
         //Establishes XY dataset for use by chart
         XYDataset xyDataset = new XYSeriesCollection(seriesSitMeanJobVTime);
         
         //instantiate a new chart with the fresh dataset
         JFreeChart sitMeanUseChart = ChartFactory.createXYAreaChart
         ("Site Mean Job Time V Time "+sit,          // Title
         "Time (sec)",                               // X-Axis label
         "Mean Job Time (ms)",                       // Y-Axis label
         xyDataset,                                  // Dataset
         PlotOrientation.VERTICAL,                   // orientation
         false,                                      // legend
         false,                                      // tooltips
         false                                       // url's
         );
         
         //sets variables for use when saving charts in
         //real time as well as final overall summary
         printableTitle = "Site MJT V Time " + sit;
         printableChart = sitMeanUseChart;
         
         /* if (printing final summary)
          *    append XHTML text and save chart
          *    as a JPEG in relevant folder
          * else
          *    remove old chart from JPanel (chartPanel),
          *    place updated chart on chartPanel,
          *    then refresh chartPanel.
          */
         if (printingAll)
         {
            //in here ONLY if printing final overall summary
            overallSummaryText += ("<a href=\"allGraphs@"+ timeNow+"/"+printableTitle+
            ".JPEG\"><img src=\"allGraphs@"+ timeNow+"/"+printableTitle+".JPEG\" alt =\""+
            printableTitle+"\" width=\"300\" height=\"200\"></img></a>\n");
            try
            {
               ChartUtilities.saveChartAsJPEG(new File("allGraphs@"+ timeNow+"/"+
               printableTitle +".JPEG"),printableChart,800,500);
            }
            catch (IOException ioe)
            {
            }
         }
         else
         {
            //remove old chart from JPanel
            if (chartPanels1.getComponentCount()>0)
               chartPanels1.remove(0);
            
            //Place updated chart on chartPanel,
            BufferedImage im = sitMeanUseChart.createBufferedImage(500,300);
            JLabel lblChart = new JLabel();
            lblChart.setIcon(new ImageIcon(im));
            JScrollPane statsScroll = new JScrollPane(lblChart);
            chartPanels1.add(statsScroll);
            
            //refresh chartPanel.
            chartPanels1.repaint();
            chartPanels1.updateUI();
         }
      }
   }
   
   /**
	* Method to add lines of output to the terminal screens
	* @param newText The text which is to be displayed
	*/
   public static void addLine(String newText)
   {
		 tabTerm.append(newText+"\n");
		 //terminalScroll.getVerticalScrollBar().setValue(terminalScroll.getVerticalScrollBar().getMaximum());
		 //terminalScroll.repaint();
		 //terminalScroll.updateUI();
   }
   
   /**
    * Method alerted when all jobs have been executed hence
    * simulation is now finished.
    */
   static public void allJobsCompleted()
   {
      GridDataThread.falsifyProcessingJobs();
      SiteDataThread.falsifyProcessingJobs();
      SEDataThread.falsifyProcessingJobs();
      CEDataThread.falsifyProcessingJobs();
      TimeThread.falsifyProcessingJobs();
      timeNow = "sim_completed";
      simPaused = true;
      simulConverse.setText("Simulation now concluded. Select File --> Produce Summary to save all statistics tables and charts.");
   }
   
   /**
    * Method to detect which node has been clicked on the imageIcon and
    * call method to display relevant statistics.
    * @param e the fired MouseEvent which alerted the MouseListener.
    */
   public void mouseClicked(MouseEvent e)
   {
      int xpos = e.getX();
      int ypos = e.getY();
      for (int i =0; i<noOfSitesWithCompOrStor; i++)
      {
         if ((xpos>=coords[i][0]&&xpos<=coords[i][0]+6)&&(ypos>=coords[i][1]&&ypos<=coords[i][1]+6))
         {
            String st = "Site"+String.valueOf(siteNumbers[i]);
            treeChoice = st;
            statsConverse.setText("Statistics for " + treeChoice);
            //tell all threads which node is being viewed
            griddt.currView(treeChoice);
            for (int j = 0;j<noOfSitesWithCompOrStor;j++)
            {
               sitedt[j].currView(treeChoice);
               sedt[j].currView(treeChoice);
            }
            for (int j = 0;j<totalNoOfCEs;j++)
            {
               cedt[j].currView(treeChoice);
            }
            //call method to establish statistics
            //for this site in stats pane.
            this.siteSelected(st);
            //de-highlight last chosen node on JTree
            tree.setSelectionPath(null);
         }
      }
   }
   /**
    * mouseEntered method is not implemented.
    * @param e MouseEvent.
    */
   public void mouseEntered(MouseEvent e)
   {
   }
   
   /**
    * mouseExited method is not implemented.
    * @param e MouseEvent.
    */
   public void mouseExited(MouseEvent e)
   {
   }
   
   /**
    * Method to call appropriate method when mouse clicked
    * Important - necessary to call showPopup method from
    * both mouse pressed and mousereleased methods as
    * e.isPopupTrigger() is platform dependant.
    * @param e MouseEvent.
    */
   public void mousePressed(MouseEvent e)
   {
      //if mouseevent is right click
      //call appropriate method
      if (e.isPopupTrigger())
      {
         this.showPopup(e);
      }
   }
   
   /**
    * Method to call appropriate method when mouse released
    * Important - necessary to call showPopup method from
    * both mouse pressed and mousereleased methods as
    * e.isPopupTrigger() is platform dependant.
    * @param e MouseEvent.
    */
   public void mouseReleased(MouseEvent e)
   {
      //if mouseevent is right click
      //call appropriate method
      if (e.isPopupTrigger())
      {
         this.showPopup(e);
      }
   }
   
   /**
    * Method to display a popup menu on stats pane JPanels.
    * @param e MouseEvent.
    */
   public void showPopup(MouseEvent e)
   {
      /* if (popupevent on histogram chart)
       *    provide menu options of save chart or 
       *    change No. of bars in histogram
       * else 
       *    rovide menu options of save chart
       */
      if (e.isPopupTrigger()&&e.getComponent()!=logicPic&&(e.getComponent()==chartPanelg2||e.getComponent()==chartPanels2||e.getComponent()==chartPanelc2))
      {
         popup = new JPopupMenu();
         fMenuSaveGraphTable = new JMenuItem("Save Chart");
         fMenuSaveGraphTable.addActionListener(this);
         popup.add(fMenuSaveGraphTable);
         if(e.getComponent()==chartPanelg2)
         {
            statMenuGridHistNo = new JMenuItem("Change No. of bars in Grid Histogram");
            statMenuGridHistNo.addActionListener(this);
            popup.add(statMenuGridHistNo);
         }
         if(e.getComponent()==chartPanels2)
         {
            statMenuSiteHistNo = new JMenuItem("Change No. of bars in Site Histograms");
            statMenuSiteHistNo.addActionListener(this);
            popup.add(statMenuSiteHistNo);
         }
         if(e.getComponent()==chartPanelc2)
         {
            statMenuCompHistNo = new JMenuItem("Change No. of bars in Comp Histograms");
            statMenuCompHistNo.addActionListener(this);
            popup.add(statMenuCompHistNo);
         }
         //show popup menu at point of click
         popup.show(e.getComponent(),
         e.getX(), e.getY());
      }
      else if (e.isPopupTrigger()&&e.getComponent()!=logicPic)
      {
         popup = new JPopupMenu();
         fMenuSaveGraphTable = new JMenuItem("Save Graph");
         fMenuSaveGraphTable.addActionListener(this);
         popup.add(fMenuSaveGraphTable);
         //show popup menu at point of click
         popup.show(e.getComponent(),
         e.getX(), e.getY());
      }
   }
   
   /**
    * mouseDragged method is not implemented.
    * @param e MouseEvent.
    */
   public void mouseDragged(MouseEvent e)
   {
   }
   
   /**
    * Method to change the appearance of the cursor if it hovers over a node
    * this shall alert the user that node is clickable.
    * @param e the fired MouseEvent which alerted the MouseMotionListener.
    */
   public void mouseMoved(MouseEvent e)
   {
      //get the location of the cursor
      int xpos = e.getX();
      int ypos = e.getY();
      for (int i =0; i<noOfSitesWithCompOrStor; i++)
      {
         //if cursor is over node in imageIcon then change cursor appearance
         //else retain/change back to default cursor
         if ((xpos>=coords[i][0]&&xpos<=coords[i][0]+6)&&(ypos>=coords[i][1]&&ypos<=coords[i][1]+6))
         {
            pane.setCursor( new Cursor( Cursor.HAND_CURSOR ) );
            i=noOfSitesWithCompOrStor;
         }
         else
         {
            pane.setCursor( new Cursor( Cursor.DEFAULT_CURSOR ) );
         }
      }
   }
   /**
    * Method to display the simulation time on timeConverse
    * @param date simulation time as a Date
    */  
   static public void setTime(Date date)
   {
      //format time stamp - of the same form but without colons as these cannot be used in filenames
      timeNow = date.toString();
      //display message containing grid (or simulation) time
      timeConverse.setText("Simulation time = "+ date);
   }
   
}
