package com.topcoder.ratings.libs.persistance;

import com.topcoder.ratings.libs.model.RatingData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Saves marathon ratings to algo_rating and long_comp_result
 */
public class MarathonDataPersistor implements DataPersistor {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private int roundId;
  private Connection conn;

  /**
   * Creates a new instance of MarathonDataPersistor
   * 
   * @param roundId the round to save to
   * @param conn    db connection to use
   */
  public MarathonDataPersistor(int roundId, Connection conn) {
    this.roundId = roundId;
    this.conn = conn;
  }

  /**
   * saves the data to the db
   * 
   * @param data new rating data
   * @throws SQLException
   */
  public void persistData(RatingData[] data) throws SQLException {
    // start by setting long_comp_result
    String sqlStr = "update long_comp_result set rated_ind = 1, " +
        "old_rating = (select rating from algo_rating " +
        "   where coder_id = long_comp_result.coder_id and algo_rating_type_id = 3), " +
        "old_vol = (select vol from algo_rating " +
        "   where coder_id = long_comp_result.coder_id and algo_rating_type_id = 3), " +
        "new_rating = ?, " +
        "new_vol = ? " +
        "where round_id = ? and coder_id = ?";
    PreparedStatement psUpdate = null;
    PreparedStatement psInsert = null;

    try {
      psUpdate = conn.prepareStatement(sqlStr);
      for (int i = 0; i < data.length; i++) {
        psUpdate.clearParameters();
        psUpdate.setInt(1, data[i].getRating());
        psUpdate.setInt(2, data[i].getVolatility());
        psUpdate.setInt(3, roundId);
        psUpdate.setInt(4, data[i].getCoderID());
        psUpdate.executeUpdate();
      }

      psUpdate.close();
      // update algo_rating
      String updateSql = "update algo_rating set rating = ?, vol = ?," +
          " round_id = ?, num_ratings = num_ratings + 1 " +
          "where coder_id = ? and algo_rating_type_id = 3";

      psUpdate = conn.prepareStatement(updateSql);

      String insertSql = "insert into algo_rating (rating, vol, round_id, coder_id, algo_rating_type_id, num_ratings) "
          +
          "values (?,?,?,?,3,1)";

      psInsert = conn.prepareStatement(insertSql);

      for (int i = 0; i < data.length; i++) {
        psUpdate.clearParameters();
        psUpdate.setInt(1, data[i].getRating());
        psUpdate.setInt(2, data[i].getVolatility());
        psUpdate.setInt(3, roundId);
        psUpdate.setInt(4, data[i].getCoderID());
        if (psUpdate.executeUpdate() == 0) {
          psInsert.clearParameters();
          psInsert.setInt(1, data[i].getRating());
          psInsert.setInt(2, data[i].getVolatility());
          psInsert.setInt(3, roundId);
          psInsert.setInt(4, data[i].getCoderID());
          psInsert.executeUpdate();
        }
      }

      // mark the round as rated
      psUpdate = conn.prepareStatement("update round set rated_ind = 1 where round_id = ?");
      psUpdate.setInt(1, roundId);
      psUpdate.executeUpdate();

    } catch (Exception e) {
      logger.error("Failed to run the Data Persistor for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);  
    } finally {
      psUpdate.close();
      psInsert.close();
    }
  }
}
