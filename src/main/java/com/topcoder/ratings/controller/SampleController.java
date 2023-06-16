package com.topcoder.ratings.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/employees")
public class SampleController {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  @GetMapping(path="/", produces = "application/json")
    public String getEmployees() 
    {
      logger.info("starting rating calculation service");
      return "Hello there";
    }  
}
