package com.topcoder.ratings.libs.loader;

import com.topcoder.ratings.libs.model.RatingData;

/**
 * Interface for class responsible for loading
 * all needed data from the DB or manual source
 */
public interface DataLoader {
    
    /**
     * Loads any data needed from the DB
     * 
     * Any parameters needed for this operation are expected to be set by this point,
     * usually via a constructor
     * @return array of loaded data
     */
    RatingData[] loadData();
    
}
