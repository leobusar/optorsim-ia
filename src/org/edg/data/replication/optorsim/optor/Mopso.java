/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.edg.data.replication.optorsim.optor;

import java.util.Random;
import org.edg.data.replication.optorsim.infrastructure.DataFile;

/**
 * <p>
 * Implementation of Mopso.
 * <p>
 * @author leobusta
 * @since JDK1.6
 */
public class Mopso {

    private boolean[][] particulas;
    private int nParticulas = 30;
    private int MaxIt = 100;
    private double w = 0.5, wdamp = 0.5;
    private DataFile[] files;
    private int leader=0;

    public Mopso(DataFile[] files) {
        this.files = files;
    }

    public void inicializarPart() {
        particulas = new boolean[nParticulas][files.length];
        Random rand = new Random();

        for (int i = 0; i < nParticulas; i++) {
            for (int j = 0; j < files.length; j++) {
                particulas[i][j] = rand.nextBoolean();
            }
        }

    }

    public int selectLeader() {
        double maxWorthValue = 0;
        Random rand = new Random();
        for (int i = 0; i < nParticulas; i++) {
            double worthValue = 0;
            for (int j = 0; j < files.length; j++) {
                if (particulas[i][j]) {
                    worthValue += files[j].lastEstimatedValue();
                }
            }
            if (maxWorthValue < worthValue || (maxWorthValue == worthValue && rand.nextBoolean())) {
                maxWorthValue = worthValue;
                leader = i;
            }
        }

        return leader;
    }

    public  boolean [] getBestFilesIndex() {
        /**
         * Inicializamos las particulas **
         */

        inicializarPart();
        selectLeader();
        for (int i = 0; i < MaxIt; i++) {
            for (int j = 0; j < nParticulas; j++) {
                updatePos(j);
                updateSpeed(j);
            }
            selectLeader();
        }

        return particulas[leader];
    }

    public void updatePos(int particulaIdx) {
        Random rand = new Random();
        //mutation
        double lastWorthValue=0,worthValue = 0;
        for (int j = 0; j < files.length; j++) {
            if (particulas[particulaIdx][j]) {
                worthValue += files[j].lastEstimatedValue();
            }
        }        
        int fileModified = rand.nextInt(files.length);
        boolean lastValue = particulas[particulaIdx][fileModified];
        particulas[particulaIdx][fileModified] = rand.nextBoolean();

        for (int j = 0; j < files.length; j++) {
            if (particulas[particulaIdx][j]) {
                worthValue += files[j].lastEstimatedValue();
            }
        }
        if (lastWorthValue > worthValue || (lastWorthValue == worthValue && rand.nextBoolean())) {
            particulas[particulaIdx][fileModified] = lastValue;
        }
    }


public void updateSpeed(int particulaIdx)
    {
        // not implemented
        // revisar la distancia al lider y de esa manera revisar cuantos archivos modificar.
        
    }
}
