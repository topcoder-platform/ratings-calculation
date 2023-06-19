package com.topcoder.ratings.controller;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.libs.process.MarathonRatingProcess;
import com.topcoder.ratings.libs.process.RatingProcess;

@RestController
@RequestMapping(path = "/ratings")
public class MarathonMatchController {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  DBHelper dbHelper;
  Connection conn;

  @PostMapping(path = "/mm/calculcate", produces = "application/json")
  public ResponseEntity<String> calculateRatings(@RequestBody Map<String, Object> body) throws SQLException {
    int roundId = Integer.parseInt(body.get("roundId").toString());

    try {
      logger.debug("Starting calculation for round " + roundId);

      dbHelper = new DBHelper();
      conn = dbHelper.getConnection();
      RatingProcess ratingProcess = getMarathonRatingProcess(roundId, conn);

      ratingProcess.runProcess();

      return new ResponseEntity<String>("Calculation Process Finished for round " + roundId, null, HttpStatus.OK);

    } catch (SQLException e) {
      logger.error("Failed to run the Marathon Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
      return new ResponseEntity<String>("Calculation Process Failed for round " + roundId, null,
          HttpStatus.BAD_REQUEST);

    } finally {
      dbHelper.closeConnection(conn);
    }
  }

  public static RatingProcess getMarathonRatingProcess(int roundId, Connection conn) {
    return new MarathonRatingProcess(roundId, conn);
  }
}