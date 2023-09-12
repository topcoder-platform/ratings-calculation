package com.topcoder.ratings.controller;

import java.sql.Connection;
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
import com.topcoder.ratings.services.coders.CoderServiceInit;

@RestController
@RequestMapping(path = "v5/ratings")
@Validated
public class CoderController {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  DBHelper dbHelper;

  @Autowired
  private CoderServiceInit coderServiceInit;
  
  Connection oltpConn;
  Connection dwConn;

  java.sql.Timestamp fStartTime = null;

  @PostMapping(path = "/coders/load", produces = "application/json")
  public ResponseEntity<Object> loadCoders(@RequestBody Map<String, Object> body) throws Exception {
    int roundId = Integer.parseInt(body.get("roundId").toString());

    Map<String, String> responseData = new HashMap<>();

    logger.info("=== start: load coders ===");
    coderServiceInit.loadCoders(roundId, dbHelper);

    responseData.put("message", "initiated the load of coders to DW");
    responseData.put("status", "success");
    return new ResponseEntity<>(responseData, null, HttpStatus.OK);
  }
}
