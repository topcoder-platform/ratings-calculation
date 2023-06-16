package com.topcoder.ratings.libs.persistance;

import com.topcoder.ratings.libs.model.RatingData;

/**
 * Interface for saving ratings data to the DB
 */
public interface DataPersistor {
    /**
     * saves data to the db
     * @param data new rating data
     */
    void persistData(RatingData[] data);
}
