package org.edg.data.replication.optorsim.infrastructure;

import java.util.*;

/**
 * This class implements a binary tree. 
 *  Used to find files in the Zipf distribution.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author ruben
 * @since JDK1.4
 */
public class BinaryTree {
	
   		private Node root;

   		private class Node {

	   		int firstKey;
	   		int secondKey;
	   		Node left, right;

	   		Node (int v1, int v2, int codeOp) {

		   		if (codeOp == 1) {  
			  		firstKey = v1;
			  		secondKey = 1;
	   			}
	   			else {
		  			firstKey = v1;
		  			secondKey = v2;
	   			}      
	  		}	  	         
   		}
     
		/**
		* @return insert a node into a binary tree
		*/
   		private Node doInsert(int v1, int v2, int codeOp, Node n) {

	   		if (n == null)
		  		return(new Node(v1,v2,codeOp));
	   		else {
	  			if (codeOp == 1) {
		 			if (v1 == n.firstKey)
						n.secondKey++;
			 		else {
						if (v1 < n.firstKey)
			   				n.left = doInsert(v1,v2,codeOp,n.left);
						else
				   			n.right = doInsert(v1,v2,codeOp,n.right);
		 			}
	  			}    	   
	  			else {   
			 		if (v1 < n.firstKey)
						n.left = doInsert(v1,v2,codeOp,n.left);
		 			else
						n.right = doInsert(v1,v2,codeOp,n.right);
	  			}	 
		  		return n;
	   		}
   		}

   		/**
		* @return a node with a given information
		*/    
   		private Node doSearchNode(int key, Node n) {
    
	   		if (n != null) {
	  			if (n.firstKey == key)
		 			return n;
	  			else {	
		 			if (n.firstKey > key)
						return doSearchNode(key,n.left);
		 			else
						return doSearchNode(key,n.right);	    
		  		}
	   		}
   			return n;
   		}

   		/**
		* @return the position of a fileId in a given ranking
		*/    
   		private int doSearchPosition(int key, Node n) {
   
			if (n != null) {
	  			if (n.firstKey == key)
		 			return n.secondKey;
	  			else {   
		 			if (n.firstKey > key)
						return doSearchPosition(key,n.left);
		 			else
						return doSearchPosition(key,n.right);	    
		  		}
	   		}
			return -1;
		}	         

		/**
		* Creates the ranking for the Zipf distribution
		*/    	          
   		private void doMakeRanking(Node n1, BinaryTree n2) {
   
			if (n1 != null) {
				doMakeRanking(n1.left,n2);
	  			int a = n1.secondKey;
	  			int b = n1.firstKey;
		  		n2.getFromHistory(a,b,2);
		  		doMakeRanking(n1.right,n2);
	   		}
   		}

		/**
		* Computes the distance of the sorted FileId's in a given ranking 
		*/
   		private void doComputeDistance(Node n1, BinaryTree n2, NodeCounter c) {
    
			Node a;
	
	   			if (n1 != null) {
		  			doComputeDistance(n1.right,n2,c);
	  				a = n2.searchNode(n1.secondKey);
	  				a.secondKey = c.getValue();  	 
	  				c.add();  
	  				doComputeDistance(n1.left,n2,c);	   
   				}
   		}	   	    	
    
		/**
		* puts the elements of a binary tree in an array
		*/
   		private void doIntoArray(Node n, int[] a, NodeCounter c) {
    
	 		if (n != null) {
				doIntoArray(n.left,a,c);
				a[c.getValue()-1] = n.firstKey;
				c.add();
				doIntoArray(n.right,a,c);
	 		}
		}      	 

		/**
		 * Search for the node corresponding to the given key.
		* @return a node with a given information
		*/
   		public Node searchNode(int key) {
    
			return doSearchNode(key,root);
   		}	    

		/**
		 * Find the position of a file ID in this binary tree.
		* @return the position of a fileId in a given ranking
		*/  
   		public int searchPosition(int key) {
    
	   		return doSearchPosition(key,root);
   		}	

   		/**
		* Gets the file information from the access history and puts 
		* in a binary tree
		*/        
   		public void getFromHistory(int v1, int v2, int codeOp) {

	   		root = doInsert(v1,v2,codeOp,root);
   		}  

   		/**
		* Creates the ranking for the Zipf distribution
		*/ 
   		public void makeRanking(BinaryTree root1) {

	 		doMakeRanking(root,root1); 
   		}

   		/**
		* Computes the distance of the sorted FileIds in a given ranking 
		*/    
   		public void computeDistance(BinaryTree root1, NodeCounter c) {

		   	doComputeDistance(root,root1,c);
   		}

		/**
		* Computes the distance of the sorted FileIds in a given ranking 
		*/    
		public int computeDistance(BinaryTree root1) {
			NodeCounter c = new NodeCounter();
			doComputeDistance(root,root1,c);
			return c.getValue();
		}

   		/**
		* Puts the elements of a binary tree in an array
		*/    
   		public void intoArray(int[] a) {
    
			NodeCounter c = new NodeCounter(); 
	   		doIntoArray(root,a,c);
   		}			
   		
		/**
		* @return the estimate the future value of a file using a Zipf distribution
		*/
		public static double estimateFutureValueZipf(int fileId, TreeMap accessHistory) {

			int i, value;
			long dt = OptorSimParameters.getInstance().getDt();
			BinaryTree t1 = new BinaryTree();
			BinaryTree t2 = new BinaryTree();

			Iterator it = accessHistory.keySet().iterator();
			while (it.hasNext()){
				i = ( (Integer)accessHistory.get(it.next()) ).intValue();
				t1.getFromHistory(i,0,1);           
			}       
			t1.makeRanking(t2);
			NodeCounter c = new NodeCounter();
			t2.computeDistance(t1,c);	
			int[] rankingS = new int[c.getValue()-1];
			t1.intoArray(rankingS); 
			value = searchFileId(rankingS,fileId,t1);
			Integer a = new Integer(value);
			Integer b = new Integer(rankingS.length+1); 
			double x = 1 - (a.doubleValue() / b.doubleValue());	
			return x;           
		}
		
		/**
		* @return the position of a fileId in a given ranking
		*/
		private static int searchFileId(int[] rankingS, int fileId, BinaryTree t) {       

			int i; 
			int low = -1;
			int high = rankingS.length-1;
			for ( ; high - low > 1; ) {
				i = (high + low) / 2;
				if (fileId <= rankingS[i])
					high = i;
				else
					low = i;
			}
			int value = 0;  
			if (fileId == rankingS[high])
				value = t.searchPosition(fileId);       
			else
				if (low == -1)
					value = Math.abs(t.searchPosition(rankingS[high]) - fileId);
				else
					value = Math.min(Math.abs(t.searchPosition(rankingS[high]) - fileId),Math.abs(t.searchPosition(rankingS[low]) - fileId));
			return value;        
		}
		
}
	
	
	/**
	* this class implements a counter for the elements of a binary tree
	*/
	class NodeCounter {
		private int value;
		public NodeCounter() {
			value = 1;
		}
		
		/**
		* increments the value of the counter
		*/    	
		public void add() {
			value++;
	   }	

	   /**
		* returns the value stored in the counter
		*/
		public int getValue() {
			return value;
		}
	}    
