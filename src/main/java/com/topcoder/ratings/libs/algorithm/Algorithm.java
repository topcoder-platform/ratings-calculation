package com.topcoder.ratings.libs.algorithm;

import com.topcoder.ratings.libs.model.RatingData;

/**
 * Represents a rating algorithm
 */
public interface Algorithm {
    
    /**
     * Gives the algorithm list of rating data to use for this run
     * @param data Array of data to use for this rating run
     */
    void setRatingData(RatingData[] data);
    
    /**
     * Returns modified rating data after run
     * @return array of modified rating data
     */
    RatingData[] getRatingData();
    
    /**
     * Runs the rating process for this algorithm
     */
    void runRatings();
    
}
