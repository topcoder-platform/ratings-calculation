package com.topcoder.ratings.controller;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.services.srm.SRMServiceInit;

@RestController
@RequestMapping(path = "v5/ratings")
@Validated
public class SRMController {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  DBHelper dbHelper;

  @Autowired
  private SRMServiceInit srmServiceInit;

  @PostMapping(path = "/srm/calculate", produces = "application/json")
  public ResponseEntity<Object> calculateSRMRatings(@RequestBody Map<String, Object> body) throws Exception {
    int roundId = Integer.parseInt(body.get("roundId").toString());

    Map<String, String> responseData = new HashMap<>();

    logger.info("Starting calculation for round " + roundId);
    srmServiceInit.calculateSRMRatings(roundId, dbHelper);

    responseData.put("message", "initiated the calculation of ratings for round " + roundId);
    responseData.put("status", "success");
    return new ResponseEntity<>(responseData, null, HttpStatus.OK);
  }
}
