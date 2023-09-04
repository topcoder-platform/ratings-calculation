package com.topcoder.ratings.services.marathonmatch;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;

import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.events.EventHelper;
import com.topcoder.ratings.libs.process.MarathonRatingProcess;
import com.topcoder.ratings.libs.process.RatingProcess;

@Service
@EnableAsync
public class MarathonServiceInit {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  Connection conn;
  Connection oltpConn;
  Connection dwConn;

  @Autowired
  EventHelper eventHelper;

  @Async
  @PostMapping(path = "/mm/calculcate", produces = "application/json")
  public void calculateRatings(int roundId, DBHelper dbHelper) throws Exception {
    try {
      logger.info("=== start rating calculate for round " + roundId + " ===");

      oltpConn = dbHelper.getConnection("OLTP");
      
      RatingProcess ratingProcess = getMarathonRatingProcess(roundId, oltpConn);
      ratingProcess.runProcess();

      logger.info("=== end rating calculate for round " + roundId + " ===");

      logger.info("=== sending message for round: " + roundId + " ===");
      eventHelper.fireEvent(roundId, "RATINGS_CALCULATION", "COMPLETE");

      logger.info("=== complete rating calculate for round " + roundId + " ===");
      
    } catch (SQLException e) {
      logger.error("Failed to run the Marathon Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeConnection(conn);
    }
  }


  @Async
  public void loadRatingsToDW(int roundId, DBHelper dbHelper) throws SQLException {
    final int MARATHON_RATING_TYPE_ID = 3;
    final int OVERALL_RATING_RANK_TYPE_ID = 1;
    final int ACTIVE_RATING_RANK_TYPE_ID = 2;

    try {
      logger.info("=== start load ratings to DW for round " + roundId + " ===");

      MarathonLoadService mmLoadService = new MarathonLoadService();
      RankService rankService = new RankService();

      java.sql.Timestamp fStartTime = new java.sql.Timestamp(System.currentTimeMillis());

      oltpConn = dbHelper.getConnection("OLTP");
      dwConn = dbHelper.getConnection("DW");

      mmLoadService.getLastUpdateTime(dwConn);
      mmLoadService.clearRound(dwConn, roundId);
      mmLoadService.loadContest(oltpConn, dwConn, roundId);
      mmLoadService.loadRound(oltpConn, dwConn, roundId);
      mmLoadService.loadProblem(oltpConn, dwConn, roundId);
      mmLoadService.loadProblemCategory(oltpConn, dwConn, roundId);
      mmLoadService.loadProblemSubmission(oltpConn, dwConn, roundId);
      mmLoadService.loadSystemTestCase(oltpConn, dwConn, roundId);
      mmLoadService.loadSystemTestResult(oltpConn, dwConn, roundId);
      mmLoadService.loadResult(oltpConn, dwConn, roundId);

      // only load the following if the round is a rated round
      if (mmLoadService.isRated(oltpConn, roundId)) {
        mmLoadService.loadRating(oltpConn, dwConn, roundId);

        // Load algo_rating_history
        mmLoadService.clearHistory(dwConn, roundId);

        Integer prevRoundId = mmLoadService.getPreviousRound(dwConn, roundId);
        if (prevRoundId != null) {
          mmLoadService.copyHistory(dwConn, prevRoundId, roundId);
        }
        mmLoadService.loadHistory(dwConn, roundId);

        // Load ranks, history has to come first because the rank loads depend on it.
        List l = rankService.getRatingsForRound(dwConn, roundId, MARATHON_RATING_TYPE_ID);

        rankService.loadRatingRank(dwConn, roundId, OVERALL_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);
        rankService.loadRatingRank(dwConn, roundId, ACTIVE_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);

        rankService.loadRatingRankHistory(dwConn, roundId, OVERALL_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);
        rankService.loadRatingRankHistory(dwConn, roundId, ACTIVE_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);

        rankService.loadCountryRatingRank(dwConn, roundId, OVERALL_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);
        rankService.loadCountryRatingRank(dwConn, roundId, ACTIVE_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);

        rankService.loadStateRatingRank(dwConn, roundId, OVERALL_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);
        rankService.loadStateRatingRank(dwConn, roundId, ACTIVE_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);

        rankService.loadSchoolRatingRank(dwConn, roundId, OVERALL_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);
        rankService.loadSchoolRatingRank(dwConn, roundId, ACTIVE_RATING_RANK_TYPE_ID, MARATHON_RATING_TYPE_ID, l);

        mmLoadService.loadStreaks(dwConn);

        logger.info("*** round is not rated, skipping rating related loads");
      } else {
        logger.info("*** round is not rated, skipping rating related loads");
      }

      mmLoadService.setLastUpdateTime(dwConn, fStartTime);
      logger.info("=== end load ratings to DW for round " + roundId + " ===");

      logger.info("=== sending message for round: " + roundId + " ===");
      eventHelper.fireEvent(roundId, "LOAD_RATINGS", "COMPLETE");

      logger.info("=== complete load ratings to DW for round " + roundId + " ===");
    } catch (Exception e) {
      logger.error("Failed to run the Marathon Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeConnection(dwConn);
      dbHelper.closeConnection(dwConn);
    }
  }
  
  public static RatingProcess getMarathonRatingProcess(int roundId, Connection conn) {
    return new MarathonRatingProcess(roundId, conn);
  }
}
