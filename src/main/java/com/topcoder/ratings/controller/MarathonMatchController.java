package com.topcoder.ratings.controller;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.topcoder.ratings.database.DBConfig;
import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.libs.process.MarathonRatingProcess;
import com.topcoder.ratings.libs.process.RatingProcess;
import com.topcoder.ratings.services.marathonmatch.MarathonLoadService;
import com.topcoder.ratings.services.marathonmatch.RankService;

@RestController
@RequestMapping(path = "v5/ratings")
public class MarathonMatchController {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  DBHelper dbHelper;
  
  Connection conn;
  Connection oltpConn;
  Connection dwConn;

  @PostMapping(path = "/mm/calculate", produces = "application/json")
  public ResponseEntity<Object> calculateRatings(@RequestBody Map<String, Object> body) throws SQLException {
    int roundId = Integer.parseInt(body.get("roundId").toString());
    Map<String, String> responseData = new HashMap<>();

    try {
      logger.debug("Starting calculation for round " + roundId);

      conn = dbHelper.getConnection("OLTP");
      RatingProcess ratingProcess = getMarathonRatingProcess(roundId, conn);

      ratingProcess.runProcess();
      
      responseData.put("message", "Calculation Process Finished for round " + roundId);
      responseData.put("status", "success");
      return new ResponseEntity<>(responseData, null, HttpStatus.OK);

    } catch (SQLException e) {
      logger.error("Failed to run the Marathon Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
      responseData.put("message", "Calculation Process Finished for round " + roundId);
      responseData.put("status", "failure");
      return new ResponseEntity<>(responseData, null, HttpStatus.BAD_REQUEST);
    } finally {
      dbHelper.closeConnection(conn);
    }
  }

  @PostMapping(path = "/mm/loadToDW", produces = "application/json")
  public ResponseEntity<Object> loadRatingsToDW(@RequestBody Map<String, Object> body) throws SQLException {
    int roundId = Integer.parseInt(body.get("roundId").toString());

    final int MARATHON_RATING_TYPE_ID = 3;
    final int OVERALL_RATING_RANK_TYPE_ID = 1;
    final int ACTIVE_RATING_RANK_TYPE_ID = 2;

    Map<String, String> responseData = new HashMap<>();

    try {
      logger.debug("Starting ratings transfer for round " + roundId);

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
      } else {
        logger.info("*** round is not rated, skipping rating related loads");
      }

      mmLoadService.setLastUpdateTime(dwConn, fStartTime);

       
      responseData.put("message", "Ratings load finished for round " + roundId);
      responseData.put("status", "success");
      return new ResponseEntity<>(responseData, null, HttpStatus.OK);
    } catch (Exception e) {
      logger.error("Failed to run the Marathon Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);

      responseData.put("message", "Ratings load failed for round " + roundId);
      responseData.put("status", "failure");
      return new ResponseEntity<>(responseData, null, HttpStatus.BAD_REQUEST);
    } finally {
      dbHelper.closeConnection(dwConn);
      dbHelper.closeConnection(dwConn);
    }
  }

  public static RatingProcess getMarathonRatingProcess(int roundId, Connection conn) {
    return new MarathonRatingProcess(roundId, conn);
  }
}