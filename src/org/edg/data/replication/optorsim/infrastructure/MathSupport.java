package org.edg.data.replication.optorsim.infrastructure;

import java.util.Random;

/**
 * This class provides computational support for arithmetic calculations
 * in OptorSim.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 */
public abstract class MathSupport{


    private static Random _random;
    private static boolean _randomSeed = OptorSimParameters.getInstance().useRandomSeed();

	static {
		if(_randomSeed) _random = new Random();
		else _random = new Random(99);
	}
	
    /**
     * Creates the permutation for the Zipf distribution
     */
    
    public static int[] makePermutation(int numFilesInSet) {
	int value;
	boolean todo;
	boolean exist[];

	int[] permutation = new int[numFilesInSet];
	exist = new boolean[numFilesInSet];
	value = 0;
	for (int i = 0; i < permutation.length; i++) {
	    todo = true;
	    while (todo) {
		value = _random.nextInt(numFilesInSet);
		todo = (exist[value] != false);
	    }
	    permutation[i] = value;
	    exist[value] = true;
	}
	return permutation;
    }

    /**
     * return a random value according to Zipf distribution
     */

    public static int zipfDistribution(int permutation[], double shape) {

		int i_res = 0;
		double res = 0;
    	boolean todo = true;

		while (todo) {	
			res = Math.pow(_random.nextDouble(), -1. / shape);
	   		i_res = (int) (res - 1);   
	   		todo = i_res >= (permutation.length - 1) || (i_res < 0);
		}
		return permutation[i_res]; 
    }


    /**
     * returns the factorial of the specified long
     */
    public static double factorial(long n){
	double fac = 1;
	if(n <= 0) return fac;
	for (int i=1; i<=n; i++) fac *= i;
	return fac;
    }

    
    private static double log_gamma( float xx) {
	double x;
	double y=x=xx;
	double tmp=x+5.5;
	tmp -= (x+0.5)*Math.log(tmp);
        double ser=1.000000000190015;
	ser += 76.18009172947146/++y;
	ser += -86.50532032941677/++y;
	ser += 24.01409824083091/++y;
	ser += -1.231739572450155/++y;
	ser += 0.1208650973866179e-2/++y;
	ser += -0.5395239384953e-5/++y;

	return -tmp+Math.log(2.5066282746310005*ser/x);
	}


    /**
     * returns the binomial coefficient (n k) of the two parameters
     */
    public static double binomialCoefficient(long n, long k){
	
	double coeff = 0;
	try{
	    coeff = factorial(n) / ( factorial(k) * factorial(n - k) );  
	}catch (ArithmeticException e){
	    e.printStackTrace();
	    System.out.println("n = "+n+"; k = "+k+".");
	}	
	return coeff;
    } 

    /**
     * Returns the logarithmic binomial of the two parameters.
     */
    public static double logBinomialCoefficient(long n, float k){
	return log_gamma(n+1) - (log_gamma(k+1) + log_gamma(1+n-k));
    }

    /**
     * Returns the value of a symmetric binomial distribution, with the
     * specified center and width, calculated for the specified value
     * of the variable
     */  
    public static double symBinDistribution(long maxWidth, float center, long value) {

	if(Math.abs(value - center) > maxWidth)
	    return 0;
	
	double log_bin = logBinomialCoefficient( 2 * maxWidth, value - center + maxWidth);
	
	return Math.exp( log_bin - 2 * maxWidth * Math.log(2));
    }

   /**
    * This method gives a random jitter which is added to the mean bandwidths taken by 
    * BandwidthReader. A Gaussian distribution is used, with the mean value mu and
    * standard deviation sigma passed from the calling routine.
    **/
    public static double addGaussianJitter( double mu, double sigma) {

		return mu + sigma * _random.nextGaussian();
    }
    
	/**
	*This method gives a random jitter which is added to the mean bandwidths taken by 
	* BandwidthReader. A reverse Landau distribution is used, with the mean value mu and
	* standard deviation sigma passed from the calling routine.
	**/
	public static double addLandauJitter(double mu, double sigma) {

		return mu + sigma * nextReverseLandau();
	}
    
    
	/**
	* This method generates a reverse Landau distribution with a most
	* probable value of zero.
	**/
	public static double nextReverseLandau() {
        
		double a = 0.1;      //Landau parameters: 0.1 gives a good realistic shape
		double b = 0.1;
		double mu = 0;  //Most probable value
		double ymax = b/a*Math.exp(-0.5);  // value of function at mu
		double x1,x2,y;

		do {
			x1 = 4 * _random.nextDouble() - 2;   // between -2.0 and 2.0   
			y = b/a*Math.exp(-0.5*((mu-x1)/a + Math.exp(-(mu-x1)/a)));
			x2 = ymax * _random.nextDouble();
		} while (x2 > y);
        
		return x1;
	}
    
}
