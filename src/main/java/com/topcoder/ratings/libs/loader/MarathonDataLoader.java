package com.topcoder.ratings.libs.loader;

import com.topcoder.ratings.libs.model.RatingData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads data for a marathon round from the db
 */
public class MarathonDataLoader implements DataLoader {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private int roundId;
  private Connection conn;

  /**
   * Creates a new instance of MarathonDataLoader
   * 
   * @param conn    db connection object to use
   * @param roundId the round ID to load
   */
  public MarathonDataLoader(int roundId, Connection conn) {
    this.roundId = roundId;
    this.conn = conn;
  }

  /**
   * loads data for the selected round
   * 
   * @return data loaded from the db
   * @throws SQLException
   */
  public RatingData[] loadData() throws SQLException {
    ArrayList<RatingData> data = new ArrayList<RatingData>();

    String sqlStr = "select lcr.coder_id, lcr.system_point_total, ar.rating, ar.vol, ar.num_ratings " +
    "from long_comp_result lcr, OUTER(algo_rating ar) " +
    "where lcr.round_id = ? " +
    "and lcr.attended = 'Y' " +
    "and ar.coder_id = lcr.coder_id " +
    "and ar.algo_rating_type_id = 3 " +
    "order by lcr.system_point_total desc";

    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = conn.prepareStatement(sqlStr);
      ps.setInt(1, roundId);
      rs = ps.executeQuery();

      while (rs.next()) {
        RatingData item = new RatingData();
        item.setCoderID(rs.getInt("coder_id"));
        item.setScore(rs.getDouble("system_point_total"));
        if (rs.getString("rating") != null) {
          item.setRating(rs.getInt("rating"));
          item.setVolatility(rs.getInt("vol"));
          item.setNumRatings(rs.getInt("num_ratings"));
        }
        data.add(item);
      }

    } catch (Exception e) {
      logger.error("Failed to run the Data Loader for round " + roundId);
      logger.error(e.getMessage());
      logger.error(e.getStackTrace().toString());  
    } finally {
      rs.close();
      ps.close();
    }

    RatingData[] ret = new RatingData[data.size()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = (RatingData) data.get(i);
    }
    return ret;
  }
}
