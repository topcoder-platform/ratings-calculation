package com.topcoder.ratings.libs.process;

import java.sql.SQLException;

import com.topcoder.ratings.libs.algorithm.Algorithm;
import com.topcoder.ratings.libs.loader.DataLoader;
import com.topcoder.ratings.libs.persistance.DataPersistor;

/**
 * Base class for a process helper that runs all the needed
 * steps in a rating run
 */
public abstract class RatingProcess {
    
    /**
     * The algorithm for this process
     */
    protected Algorithm algo;
    /**
     * The data loader for this process
     */
    protected DataLoader loader;
    
    /**
     * persistor for this process
     */
    protected DataPersistor persistor;
    
    /**
     * Runs the rating process
     * @throws SQLException
     */
    public abstract String runProcess() throws Exception;
    
}
