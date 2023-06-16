package com.topcoder.ratings.libs.process;

import com.topcoder.ratings.libs.algorithm.AlgorithmQubits;
import com.topcoder.ratings.libs.loader.MarathonDataLoader;
import com.topcoder.ratings.libs.model.RatingData;
import com.topcoder.ratings.libs.persistance.MarathonDataPersistor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Process helper object for rating a marathon round
*/
public class MarathonRatingProcess extends RatingProcess {
    
    private int roundId;
    private Connection conn;
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    /**
     * Creates a new instance of MarathonRatingProcess
     * @param roundId the round to rate
     * @param conn the db connection to use
     */
    public MarathonRatingProcess(int roundId, Connection conn) {
        this.roundId = roundId;
        this.conn = conn;
        this.algo = new AlgorithmQubits();
        this.loader = new MarathonDataLoader(roundId, conn);
        this.persistor = new MarathonDataPersistor(roundId, conn);
    }

    /**
     * Loads all data, rates the round, then saves data to the DB
     */
    public void runProcess() {
        log.info("Starting run for round " + roundId);
        
        log.debug("Getting Data");
        RatingData[] data = loader.loadData();
        log.debug("Data loaded (" + data.length + ")");
        
        //split the data into provisional and non-provisional groups
        ArrayList provDataList = new ArrayList();
        //the provisional group is everyone at first
        provDataList.addAll(Arrays.asList(data));
        
        RatingData[] provData = (RatingData[])provDataList.toArray(new RatingData[0]);
        
        //non prov data removes non-rated people
        ArrayList nonprovDataList = new ArrayList();
        for(int i = 0; i < data.length; i++) {
            if(data[i].getNumRatings() != 0)
                nonprovDataList.add(data[i]);
        }
        
        RatingData[] nonprovData = (RatingData[])nonprovDataList.toArray(new RatingData[0]);
        
        //run the algorithm (provisional)        
        algo.setRatingData(provData);
        algo.runRatings();
        provData = algo.getRatingData();

        log.debug("Algorithm Run (provisional)");
        
        //remove non-prov coders
        provDataList = new ArrayList();
        for(int i = 0; i < provData.length; i++) {
            if(provData[i].getNumRatings() == 1)
                provDataList.add(provData[i]);
        }
        
        RatingData[] provDataFiltered = (RatingData[])provDataList.toArray(new RatingData[0]);;
        
        //persist
        persistor.persistData(provDataFiltered);
        log.debug("Data Saved (provisional)");
        
        //run the algorithm (provisional)        
        algo.setRatingData(nonprovData);
        algo.runRatings();
        nonprovData = algo.getRatingData();

        log.debug("Algorithm Run (non-provisional)");
        
        //persist
        persistor.persistData(nonprovData);
        log.debug("Data Saved (non-provisional)");
        
        
    }
    
}
