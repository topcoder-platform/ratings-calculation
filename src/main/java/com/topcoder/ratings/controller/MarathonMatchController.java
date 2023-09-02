package com.topcoder.ratings.controller;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.services.marathonmatch.MarathonServiceInit;

@RestController
@RequestMapping(path = "v5/ratings")
public class MarathonMatchController {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  DBHelper dbHelper;

  @Autowired
  private MarathonServiceInit marathonServiceInit;

  @PostMapping(path = "/mm/calculcate", produces = "application/json")
  public ResponseEntity<Object> calculateRatings(Authentication userAuth, @RequestBody Map<String, Object> body) throws Exception {
    int roundId = Integer.parseInt(body.get("roundId").toString());

    Map<String, String> responseData = new HashMap<>();

    logger.info("Starting calculation for round " + roundId);
    marathonServiceInit.calculateRatings(roundId, dbHelper);

    responseData.put("message", "initiated the calculation of ratings for round " + roundId);
    responseData.put("status", "success");
    return new ResponseEntity<>(responseData, null, HttpStatus.OK);
  }

  @PostMapping(path = "/mm/loadToDW", produces = "application/json")
  public ResponseEntity<Object> loadRatingsToDW(@RequestBody Map<String, Object> body) throws SQLException {
    int roundId = Integer.parseInt(body.get("roundId").toString());

    Map<String, String> responseData = new HashMap<>();

    logger.info("Ratings load process started for round " + roundId);
    marathonServiceInit.loadRatingsToDW(roundId, dbHelper);

    responseData.put("message", "initiated the loading of ratings to DW for round " + roundId);
    responseData.put("status", "success");
    return new ResponseEntity<>(responseData, null, HttpStatus.OK);
  }
}