package com.topcoder.ratings.services.coders;

import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.events.EventHelper;

@Service
@EnableAsync
public class CoderServiceInit {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  Connection oltpConn;
  Connection dwConn;

  @Autowired
  EventHelper eventHelper;

  java.sql.Timestamp fStartTime = null;

  @Async
  public void loadCoders(int roundId, DBHelper dbHelper) throws Exception {

    fStartTime = new java.sql.Timestamp(System.currentTimeMillis());

    CoderService coderService = new CoderService();

    try {
      logger.info("=== start load coders ===");

      oltpConn = dbHelper.getConnection("OLTP");
      dwConn = dbHelper.getConnection("DW");

      coderService.getLastUpdateTime(dwConn);
      coderService.loadState(dwConn, oltpConn);
      coderService.loadCountry(dwConn, oltpConn);
      coderService.loadCoder(dwConn, oltpConn);
      coderService.loadSkillType(dwConn, oltpConn);
      coderService.loadSkill(dwConn, oltpConn);
      coderService.loadCoderSkill(dwConn, oltpConn);
      coderService.loadRating(dwConn, oltpConn);
      coderService.loadPath(dwConn, oltpConn);
      coderService.loadImage(dwConn, oltpConn);
      coderService.loadCoderImageXref(dwConn, oltpConn);
      coderService.loadSchool(dwConn, oltpConn);
      coderService.loadCurrentSchool(dwConn, oltpConn);
      coderService.loadAchievements(dwConn, oltpConn);
      coderService.loadTeam(dwConn, oltpConn);
      coderService.loadTeamCoderXref(dwConn, oltpConn);
      coderService.loadEvent(dwConn, oltpConn);
      coderService.loadEventRegistration(dwConn, oltpConn);
      // disabling load user notification service as it is throwing error
      // and also, looks like it is not impacting otherwise
      // coderService.loadUserNotifications(dwConn, oltpConn);
      coderService.setLastUpdateTime(fStartTime, dwConn);

      logger.info("=== end load coders ===");

      logger.info("=== sending message ===");
      eventHelper.fireEvent(roundId, "LOAD_CODERS", "COMPLETE");

      logger.info("=== complete load coders ===");

    } catch (Exception e) {
      logger.error("failed to load coders");
      logger.error(e.getMessage());
      logger.error("", e);
      


    } finally {
      dbHelper.closeConnection(dwConn);
      dbHelper.closeConnection(dwConn);
    }
  }
}
