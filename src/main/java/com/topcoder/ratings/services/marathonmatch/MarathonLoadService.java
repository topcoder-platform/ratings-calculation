package com.topcoder.ratings.services.marathonmatch;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.topcoder.ratings.database.DBHelper;

public class MarathonLoadService {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  protected int ROUND_LOG_TYPE = 1;
  protected int CODING_SEGMENT_ID = 2;
  protected int MARATHON_RATING_TYPE_ID = 3;

  protected int ROUND_TYPE_MARATHON_TOURNAMENT = 19;
  protected int MARATHON_CONSECUTIVE_TOP_5 = 10;
  protected int MARATHON_CONSECUTIVE_TOP_10 = 11;

  protected String ACTIVE_TEST_STATUS = "1";

  protected java.sql.Timestamp fLastLogTime = null;
  private Hashtable<Integer, Date> fRoundStartHT = new Hashtable<Integer, Date>();

  DBHelper dbHelper = new DBHelper();

  public void getLastUpdateTime(Connection dbConnection) throws Exception {
    Statement stmt = null;
    ResultSet rs = null;
    StringBuffer query = null;

    logger.info("=== start: getLastUpdateTime ===");
    try {
      query = new StringBuffer(100);
      query.append("select timestamp from update_log where log_id = ");
      query.append("(select max(log_id) from update_log where log_type_id = " + ROUND_LOG_TYPE + ")");
      stmt = dbConnection.createStatement();
      rs = stmt.executeQuery(query.toString());
      if (rs.next()) {
        fLastLogTime = rs.getTimestamp(1);
      } else {
        throw new SQLException("Last log time not found in update_log table");
      }
    } catch (SQLException e) {
      logger.error("Failed to retrieve last log time");
      logger.error(e.getMessage());
      logger.error("", e);
      throw new Exception("Failed to retrieve last log time\n" + e.getMessage());
    }
  }

  public void clearRound(Connection dbConnection, int roundId) throws Exception {
    PreparedStatement ps = null;
    ArrayList<String> a = null;

    logger.info("=== start: clearRound ===");
    try {
      a = new ArrayList<String>();

      a.add("DELETE FROM system_test_case WHERE problem_id in (SELECT problem_id FROM problem WHERE round_id = ?)");
      a.add("DELETE FROM long_system_test_result WHERE round_id = ?");
      a.add("DELETE FROM long_problem_submission WHERE round_id = ?");
      a.add(
          "DELETE FROM problem_category_xref where problem_id in (select problem_id from problem where round_id = ?)");
      a.add("DELETE FROM problem WHERE round_id = ?");
      a.add("DELETE FROM long_comp_result WHERE round_id = ?");

      int count = 0;
      for (int i = 0; i < a.size(); i++) {
        ps = dbConnection.prepareStatement((String) a.get(i));
        if (((String) a.get(i)).indexOf('?') > -1)
          ps.setInt(1, roundId);
        count = ps.executeUpdate();
        logger.info("" + count + " rows: " + (String) a.get(i));
      }
    } catch (Exception e) {
      logger.error("clearing data failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
      throw new Exception("Failed clear data for round \n" + e.getMessage());
    }
  }

  public void loadContest(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psSel2 = null;
    PreparedStatement psUpd = null;
    ResultSet rs = null;
    ResultSet rs2 = null;
    StringBuffer query = null;

    logger.info("=== start: loadContest ===");
    try {
      query = new StringBuffer(100);
      query.append("SELECT c.contest_id "); // 1
      query.append("       ,c.name "); // 2
      query.append("       ,c.start_date "); // 3
      query.append("       ,c.end_date "); // 4
      query.append("       ,c.status "); // 5
      query.append("       ,c.group_id "); // 6
      query.append("  FROM contest c ");
      query.append("       ,round r ");
      query.append(" WHERE r.round_id = ? ");
      query.append("   AND r.contest_id = c.contest_id");
      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO contest ");
      query.append("      (contest_id "); // 1
      query.append("       ,name "); // 2
      query.append("       ,start_date "); // 3
      query.append("       ,end_date "); // 4
      query.append("       ,status "); // 5
      query.append("       ,group_id) "); // 6
      query.append("VALUES (");
      query.append("?,?,?,?,?,?)"); // 10 values
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("UPDATE contest ");
      query.append("   SET name = ? "); // 1
      query.append("       ,start_date = ? "); // 2
      query.append("       ,end_date = ? "); // 3
      query.append("       ,status = ? "); // 4
      query.append("       ,group_id = ? "); // 5
      query.append(" WHERE contest_id = ? "); // 6
      psUpd = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("SELECT 'pops' ");
      query.append("  FROM contest ");
      query.append(" WHERE contest_id = ?");
      psSel2 = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        int contest_id = rs.getInt(1);
        psSel2.clearParameters();
        psSel2.setInt(1, contest_id);
        rs2 = psSel2.executeQuery();

        // If next() returns true that means this row exists. If so,
        // we update. Otherwise, we insert.
        if (rs2.next()) {
          psUpd.clearParameters();
          psUpd.setString(1, rs.getString(2)); // name
          psUpd.setTimestamp(2, rs.getTimestamp(3)); // start_date
          psUpd.setTimestamp(3, rs.getTimestamp(4)); // end_date
          psUpd.setString(4, rs.getString(5)); // status
          psUpd.setInt(5, rs.getInt(6)); // group_id
          psUpd.setInt(6, rs.getInt(1)); // contest_id

          retVal = psUpd.executeUpdate();
          count += retVal;
          if (retVal != 1) {
            throw new SQLException(
                "TCLoadRound: Insert for contest_id " + contest_id + " modified " + retVal + " rows, not one");
          }
        } else {
          psIns.clearParameters();
          psIns.setInt(1, rs.getInt(1)); // contest_id
          psIns.setString(2, rs.getString(2)); // name
          psIns.setTimestamp(3, rs.getTimestamp(3)); // start_date
          psIns.setTimestamp(4, rs.getTimestamp(4)); // end_date
          psIns.setString(5, rs.getString(5)); // status
          psIns.setInt(6, rs.getInt(6)); // group_id

          retVal = psIns.executeUpdate();
          count += retVal;
          if (retVal != 1) {
            throw new SQLException("TCLoadRound: Insert for contest_id " + contest_id +
                " modified " + retVal + " rows, not one");
          }
        }

        dbHelper.closeResultSet(rs2);
        logger.info("... loaded " + count + " rows for contest table ...");
      }

      logger.info("contest records copied = " + count);
    } catch (Exception e) {
      logger.error("load of 'contest' table failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
      throw new Exception("load of 'contest' table failed for round \n" + e.getMessage());
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeResultSet(rs2);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psSel2);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psUpd);
    }
  }

  public void loadRound(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psSelRatingOrder = null;
    PreparedStatement psSelRatingOrderNewRound = null;
    PreparedStatement psSel2 = null;
    PreparedStatement psIns = null;
    PreparedStatement psUpd = null;
    ResultSet rs = null;
    ResultSet rs2 = null;
    ResultSet rsRatings = null;
    StringBuffer query = null;

    logger.info("=== start: loadRound ===");

    try {
      query = new StringBuffer(100);
      query.append("SELECT r.round_id "); // 1
      query.append("       ,r.contest_id "); // 2
      query.append("       ,r.name "); // 3
      query.append("       ,r.status "); // 4
      query.append("       ,c.start_date "); // 5     // c.start_date
      query.append("       ,r.round_type_id "); // 6
      query.append("       ,r.invitational "); // 7
      query.append("       ,r.notes "); // 8
      query.append("       ,(SELECT rtlu.round_type_desc "); // 9
      query.append("           FROM round_type_lu rtlu ");
      query.append("          WHERE rtlu.round_type_id = r.round_type_id) ");
      query.append("       ,r.short_name "); // 10
      query.append("       ,r.forum_id "); // 11
      query.append("       ,r.rated_ind "); // 12
      query.append("  FROM round r ");
      query.append("       ,contest c ");    // change to contest
      query.append(" WHERE r.round_id = ? ");
      query.append("   AND c.contest_id = r.contest_id ");
      // query.append("   AND rs.segment_id = " + CODING_SEGMENT_ID);
      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" SELECT max(r1.rating_order)  ");
      query.append("         from round r1, round r2 ");
      query.append("         , round_type_lu rt1 ");
      query.append("         , round_type_lu rt2 ");
      query.append("         where  r2.round_id = ? ");
      query.append("         AND r1.rated_ind = 1 ");
      query.append("         AND r2.rated_ind = 1 ");
      query.append("         AND r1.round_type_id = rt1.round_type_id ");
      query.append("         AND r2.round_type_id = rt2.round_type_id ");
      query.append("         AND rt1.algo_rating_type_id = rt2.algo_rating_type_id ");
      query.append("         AND ((r1.calendar_id < r2.calendar_id) ");
      query.append("         OR (r1.calendar_id = r2.calendar_id AND r1.time_id < r2.time_id) ");
      query.append(
          "         OR  (r1.calendar_id = r2.calendar_id AND r1.time_id = r2.time_id AND r1.round_id < r2.round_id)) ");
      psSelRatingOrder = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" SELECT max(r.rating_order)  ");
      query.append("         from round r ");
      query.append("         , round_type_lu rt ");
      query.append("         where r.rated_ind = 1 ");
      query.append("         AND r.round_type_id = rt.round_type_id ");
      query.append("         AND rt.algo_rating_type_id = " + MARATHON_RATING_TYPE_ID);
      psSelRatingOrderNewRound = dwConn.prepareStatement(query.toString());

      // We have 8 values in the insert as opposed to 7 in the select
      // because we want to provide a default value for failed. We
      // don't have a place to select failed from in the transactional
      // DB
      query = new StringBuffer(100);
      query.append("INSERT INTO round ");
      query.append("      (round_id "); // 1
      query.append("       ,contest_id "); // 2
      query.append("       ,name "); // 3
      query.append("       ,status "); // 4
      query.append("       ,calendar_id "); // 5
      query.append("       ,failed "); // 6
      query.append("       ,round_type_id "); // 7
      query.append("       ,invitational  "); // 8
      query.append("       ,notes         "); // 9
      query.append("       ,round_type_desc "); // 10
      query.append("       ,short_name "); // 11
      query.append("       ,forum_id"); // 12
      query.append("       ,rated_ind"); // 13
      query.append("       ,time_id "); // 14
      query.append("       ,rating_order)"); // 15
      query.append("VALUES (");
      query.append("?,?,?,?,?,?,?,?,?,");
      query.append("?,?,?,?,?,?)");

      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("UPDATE round ");
      query.append("   SET contest_id = ? "); // 1
      query.append("       ,name = ? "); // 2
      query.append("       ,status = ? "); // 3
      query.append("       ,calendar_id = ? "); // 4
      query.append("       ,failed = ? "); // 5
      query.append("       ,round_type_id = ? "); // 6
      query.append("       ,invitational  = ? "); // 7
      query.append("       ,notes = ?         "); // 8
      query.append("       ,round_type_desc = ? "); // 9
      query.append("       ,short_name = ? "); // 10
      query.append("       ,forum_id = ? "); // 11
      query.append("       ,rated_ind = ?"); // 12
      query.append("       ,time_id = ?"); // 13
      query.append("       ,rating_order = ?"); // 14
      query.append(" WHERE round_id = ? "); // 15
      psUpd = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("SELECT 'pops' FROM round where round_id = ?");
      psSel2 = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();
      boolean newRound = false;
      while (rs.next()) {
        int round_id = rs.getInt("round_id");

        psSel2.clearParameters();
        psSel2.setInt(1, round_id);
        rs2 = psSel2.executeQuery();

        newRound = !rs2.next();

        int ratingOrder = 0;

        // if the load is saving the round for the first time, we can't
        // get the latest round before the current because it simply
        // doesn't exist yet in the database,
        // so we get the latest marathon type rating order.
        if (newRound) {
          psSelRatingOrderNewRound.clearParameters();
          rsRatings = psSelRatingOrderNewRound.executeQuery();
          ratingOrder = rsRatings.next() ? rsRatings.getInt(1) : 0;
          if (rs.getInt("rated_ind") == 1)
            ratingOrder++;
        } else {
          psSelRatingOrder.clearParameters();
          psSelRatingOrder.setInt(1, round_id);
          rsRatings = psSelRatingOrder.executeQuery();
          ratingOrder = rsRatings.next() ? rsRatings.getInt(1) : 0;
          if (rs.getInt("rated_ind") == 1)
            ratingOrder++;
        }
        dbHelper.closeResultSet(rsRatings);

        // Retrieve the calendar_id for the start_time of this round
        java.sql.Timestamp stamp = rs.getTimestamp("start_date");
        int calendar_id = lookupCalendarId(stamp, dwConn);
        int time_id = lookupTimeId(stamp, dwConn);

        // If next() returns true that means this row exists. If so,
        // we update. Otherwise, we insert.
        if (!newRound) {
          psUpd.clearParameters();
          psUpd.setInt(1, rs.getInt("contest_id")); // contest_id
          psUpd.setString(2, rs.getString("name")); // name
          psUpd.setString(3, rs.getString("status")); // status
          psUpd.setInt(4, calendar_id); // cal_id of start_time
          psUpd.setInt(5, 0); // failed (default is 0)
          psUpd.setInt(6, rs.getInt("round_type_id")); // round_type_id
          psUpd.setInt(7, rs.getInt("invitational")); // invitational
          psUpd.setString(8, rs.getString("notes")); // notes
          psUpd.setString(9, rs.getString(10)); // round_type_desc
          psUpd.setString(10, rs.getString("short_name")); // shortname
          psUpd.setInt(11, rs.getInt("forum_id")); // forum_id
          psUpd.setInt(12, rs.getInt("rated_ind")); // rated_ind
          psUpd.setInt(13, time_id);
          if (ratingOrder > 0) {
            psUpd.setInt(14, ratingOrder);
          } else {
            psUpd.setNull(14, Types.INTEGER);
          }
          psUpd.setInt(15, rs.getInt(1)); // round_id

          retVal = psUpd.executeUpdate();
          count += retVal;
          if (retVal != 1) {
            throw new SQLException("TCLoadRound: Update for round_id " +
                round_id +
                " modified " + retVal + " rows, not one.");
          }
        } else {
          psIns.clearParameters();
          psIns.setInt(1, round_id); // round_id
          psIns.setInt(2, rs.getInt("contest_id")); // contest_id
          psIns.setString(3, rs.getString("name")); // name
          psIns.setString(4, rs.getString("status")); // status
          psIns.setInt(5, calendar_id); // cal_id of start_time
          psIns.setInt(6, 0); // failed (default is 0)
          psIns.setInt(7, rs.getInt("round_type_id")); // round_type_id
          psIns.setInt(8, rs.getInt("invitational")); // invitational
          psIns.setString(9, rs.getString("notes")); // notes
          psIns.setString(10, rs.getString(10)); // round_type_desc
          psIns.setString(11, rs.getString("short_name")); // short name
          psIns.setString(12, rs.getString("forum_id")); // forum_id
          psIns.setInt(13, rs.getInt("rated_ind")); // rating ind
          psIns.setInt(14, time_id);
          psIns.setInt(15, ratingOrder);

          retVal = psIns.executeUpdate();
          count += retVal;
          if (retVal != 1) {
            throw new SQLException("TCLoadRound: Insert for round_id " +
                round_id +
                " modified " + retVal + " rows, not one.");
          }
        }

        dbHelper.closeResultSet(rs2);
        logger.info("... loaded " + count + " rows for round table ...");
      }

      logger.info("round records copied = " + count);
    } catch (Exception e) {
      logger.error("load of round table failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs2);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psSel2);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psUpd);
      dbHelper.closeStatement(psSelRatingOrder);
    }
  }

  public void loadProblem(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psSel2 = null;
    PreparedStatement psIns = null;
    PreparedStatement psUpd = null;
    ResultSet rs = null;
    ResultSet rs2 = null;
    StringBuffer query = null;

    logger.info("=== start: loadProblem ===");

    try {
      query = new StringBuffer(100);
      query.append("SELECT p.problem_id "); // 1
      query.append("       ,rp.round_id "); // 2
      query.append("       ,c.result_type_id "); // 3
      query.append("       ,c.method_name "); // 4
      query.append("       ,c.class_name "); // 5
      query.append("       ,p.status_id "); // 6
      query.append("       ,c.default_solution "); // 7
      query.append("       ,c.component_text "); // 8
      query.append("       ,CURRENT "); // 9
      query.append("       ,(SELECT data_type_desc "); // 10
      query.append("           FROM data_type ");
      query.append("          WHERE data_type_id = c.result_type_id) ");
      query.append("       ,d.difficulty_id "); // 11
      query.append("       ,d.difficulty_desc "); // 12
      query.append("       ,rp.division_id "); // 13
      query.append("       ,rp.points "); // 14
      query.append("  FROM problem p ");
      query.append("       ,round_component rp ");
      query.append("       ,difficulty d ");
      query.append("       ,component c ");
      query.append(" WHERE rp.round_id = ? ");
      query.append("   AND p.problem_id = c.problem_id");
      query.append("   AND c.component_id = rp.component_id ");
      query.append("   AND rp.difficulty_id = d.difficulty_id");
      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO problem ");
      query.append("      (problem_id "); // 1
      query.append("       ,round_id "); // 2
      query.append("       ,result_type_id "); // 3
      query.append("       ,method_name "); // 4
      query.append("       ,class_name "); // 5
      query.append("       ,status "); // 6
      query.append("       ,default_solution "); // 7
      query.append("       ,problem_text "); // 8
      query.append("       ,modify_date "); // 9
      query.append("       ,result_type_desc "); // 10
      query.append("       ,level_id "); // 11
      query.append("       ,level_desc "); // 12
      query.append("       ,division_id "); // 13
      query.append("       ,points "); // 14
      query.append("       ,viewable) "); // 15
      query.append("VALUES (");
      query.append("?,?,?,?,?,?,?,?,?,?,");
      query.append("?,?,?,?,?)");
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("UPDATE problem ");
      query.append("   SET result_type_id = ? "); // 1
      query.append("       ,method_name = ? "); // 2
      query.append("       ,class_name = ? "); // 3
      query.append("       ,status = ? "); // 4
      query.append("       ,default_solution = ? "); // 5
      query.append("       ,problem_text = ? "); // 6
      query.append("       ,modify_date = ? "); // 7
      query.append("       ,result_type_desc = ? "); // 8
      query.append("       ,level_id = ? "); // 9
      query.append("       ,level_desc = ? "); // 10
      query.append("       ,points = ? "); // 11
      query.append("       ,viewable = ?"); // 12
      query.append(" WHERE problem_id = ? "); // 13
      query.append("   AND round_id = ? "); // 14
      query.append("   AND division_id = ? "); // 15
      psUpd = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("SELECT 'pops' FROM problem ");
      query.append(" WHERE problem_id = ? ");
      query.append("   AND round_id = ?");
      query.append("   AND division_id = ?");
      psSel2 = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        int problem_id = rs.getInt(1);
        int round_id = rs.getInt(2);
        int division_id = rs.getInt(13);

        psSel2.clearParameters();
        psSel2.setInt(1, problem_id);
        psSel2.setInt(2, round_id);
        psSel2.setInt(3, division_id);
        rs2 = psSel2.executeQuery();

        // If next() returns true that means this row exists. If so,
        // we update. Otherwise, we insert.
        if (rs2.next()) {
          psUpd.clearParameters();
          psUpd.setInt(1, rs.getInt(3)); // result_type_id
          psUpd.setString(2, rs.getString(4)); // method_name
          psUpd.setString(3, rs.getString(5)); // class_name
          psUpd.setInt(4, rs.getInt(6)); // status
          setBytes(psUpd, 5, getBytes(rs, 7)); // default_solution
          setBytes(psUpd, 6, getBytes(rs, 8)); // problem_text
          psUpd.setTimestamp(7, rs.getTimestamp(9)); // modify_date
          psUpd.setString(8, rs.getString(10)); // result_type_desc
          psUpd.setInt(9, rs.getInt(11)); // level_id
          psUpd.setString(10, rs.getString(12)); // level_desc
          psUpd.setDouble(11, rs.getDouble(14)); // points
          psUpd.setInt(12, 1); // viewable
          psUpd.setInt(13, rs.getInt(1)); // problem_id
          psUpd.setInt(14, rs.getInt(2)); // round_id
          psUpd.setInt(15, rs.getInt(13)); // division_id

          retVal = psUpd.executeUpdate();
          count += retVal;
          if (retVal != 1) {
            throw new SQLException("TCLoadRound: Update for problem_id " +
                problem_id + ", round_id " + round_id +
                " modified " + retVal + " rows, not one.");
          }
        } else {
          psIns.clearParameters();
          psIns.setInt(1, rs.getInt(1)); // problem_id
          psIns.setInt(2, rs.getInt(2)); // round_id
          psIns.setInt(3, rs.getInt(3)); // result_type_id
          psIns.setString(4, rs.getString(4)); // method_name
          psIns.setString(5, rs.getString(5)); // class_name
          psIns.setInt(6, rs.getInt(6)); // status
          setBytes(psIns, 7, getBytes(rs, 7)); // default_solution
          setBytes(psIns, 8, getBytes(rs, 8)); // problem_text
          psIns.setTimestamp(9, rs.getTimestamp(9)); // modify_date
          psIns.setString(10, rs.getString(10)); // result_type_desc
          psIns.setInt(11, rs.getInt(11)); // level_id
          psIns.setString(12, rs.getString(12)); // level_desc
          psIns.setInt(13, rs.getInt(13)); // division_id
          psIns.setDouble(14, rs.getDouble(14)); // points
          psIns.setInt(15, 1); // viewable

          retVal = psIns.executeUpdate();
          count += retVal;
          if (retVal != 1) {
            throw new SQLException("TCLoadRound: Insert for problem_id " +
                problem_id + ", round_id " + round_id +
                " modified " + retVal + " rows, not one.");
          }
        }

        dbHelper.closeResultSet(rs2);
        logger.info("... loaded " + count + " rows for problem table ...");
      }

      logger.info("problem records copied = " + count);
    } catch (Exception e) {
      logger.error("load of problem table failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeResultSet(rs2);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psSel2);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psUpd);
    }
  }

  public void loadProblemCategory(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psDel = null;
    ResultSet rs = null;
    StringBuffer query = null;

    logger.info("=== start: loadProblemCategory ===");
    try {
      query = new StringBuffer(100);
      query.append(" select distinct p.problem_id");
      query.append("      , cc.component_category_id");
      query.append(" from problem p");
      query.append(" , component c");
      query.append(" , component_category_xref cc");
      query.append(" , round_component rc");
      query.append(" where cc.component_id = c.component_id");
      query.append(" and c.problem_id = p.problem_id");
      query.append(" and c.component_id = rc.component_id");
      query.append(" and rc.round_id = ?");

      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO problem_category_xref");
      query.append("      (problem_id "); // 1
      query.append("       ,problem_category_id) "); // 2
      query.append("VALUES (");
      query.append("?,?)"); // 2 values
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("DELETE FROM problem_category_xref");
      query.append(" WHERE problem_id in ( ");
      query.append(" select problem_id from problem where round_id = ?)");
      psDel = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      // First thing we do is delete all the challenge entries for this round
      psDel.setInt(1, roundId);
      psDel.executeUpdate();

      while (rs.next()) {
        psIns.clearParameters();
        psIns.setLong(1, rs.getLong(1)); // problem_id
        psIns.setLong(2, rs.getLong(2)); // problem_category_id

        retVal = psIns.executeUpdate();
        count += retVal;
        if (retVal != 1) {
          throw new SQLException("TCLoadRound: Insert for prbolem_id " + rs.getLong(1) + " problem_category_id "
              + rs.getLong(2) + " modified " + retVal + " rows, not one.");
        }

        logger.info("... loaded " + count + " rows for problem_category_xref table ...");
      }

      logger.info("problem Category records copied = " + count);
    } catch (Exception e) {
      logger.error("load of problem problem_category_xref failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  public void loadProblemSubmission(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psDel = null;
    ResultSet rs = null;
    StringBuffer query = null;

    logger.info("=== start: loadProblemSubmission ===");
    try {
      query = new StringBuffer(100);
      query.append(" SELECT cs.round_id"); // 1
      query.append(" ,cs.coder_id "); // 2
      query.append(" , (SELECT cm.problem_id FROM component cm WHERE cm.component_id = cs.component_id)"); // 3
      query.append(" ,cs.points "); // 4
      query.append(" ,cs.status_id "); // 5
      query.append(" ,CASE WHEN s.language_id is null THEN c.language_id ELSE s.language_id END as language_id"); // 6
      query.append(" ,s.open_time "); // 7
      query.append(" ,cs.submission_number "); // 8
      query.append(" ,s.submission_text "); // 9
      query.append(" ,s.submit_time "); // 10
      query.append(" ,s.submission_points "); // 11
      query.append("  ,(SELECT status_desc "); // 12
      query.append(" FROM problem_status_lu ");
      query.append(" WHERE problem_status_id = cs.status_id) ");
      query.append(" ,c.compilation_text"); // 13
      query.append(" ,s.submission_number"); // 14
      query.append(" , s.example");
      query.append(" FROM long_component_state cs ");
      query.append("     , outer long_submission s");
      query.append("     , outer long_compilation c");
      query.append(" WHERE cs.round_id = ?");
      query.append("  and cs.long_component_state_id = c.long_component_state_id");
      query.append("   and cs.long_component_state_id = s.long_component_state_id");
      query.append("   AND NOT EXISTS ");
      query.append("       (SELECT 'pops' ");
      query.append("          FROM user_group_xref ugx ");
      query.append("         WHERE ugx.login_id= cs.coder_id ");
      query.append("           AND ugx.group_id = 2000115)");
      query.append("   AND NOT EXISTS ");
      query.append("       (SELECT 'pops' ");
      query.append("          FROM group_user gu ");
      query.append("         WHERE gu.user_id = cs.coder_id ");
      query.append("           AND gu.group_id = 13)");

      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO long_problem_submission ");
      query.append("      (round_id "); // 1
      query.append("       ,coder_id "); // 2
      query.append("       ,problem_id "); // 3
      query.append("       ,final_points "); // 4
      query.append("       ,status_id "); // 5
      query.append("       ,language_id "); // 6
      query.append("       ,open_time "); // 7
      query.append("       ,submission_number "); // 8
      query.append("       ,submission_text "); // 9
      query.append("       ,submit_time "); // 10
      query.append("       ,submission_points "); // 11
      query.append("       ,status_desc "); // 12
      query.append("       ,last_submission "); // 13
      query.append("       ,example) "); // 14
      query.append("VALUES (");
      query.append("?,?,?,?,?,?,?,?,?,?,"); // 10
      query.append("?,?,?,?)"); // 14 total values
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("DELETE FROM long_problem_submission ");
      query.append(" WHERE round_id = ? ");
      query.append("   AND coder_id = ? ");
      query.append("   AND problem_id = ?");
      query.append("   AND submission_number = ?");
      query.append("   and example = ?");
      psDel = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        long round_id = rs.getInt(1);
        long coder_id = rs.getInt(2);
        long problem_id = rs.getInt(3);
        int submission_number = rs.getInt(14);
        int example = rs.getInt("example");
        int last_submission = 0;
        if (rs.getInt(8) > 0) { // they submitted at least once
          last_submission = rs.getInt(8) == submission_number ? 1 : 0;
        }

        psDel.clearParameters();
        psDel.setLong(1, round_id);
        psDel.setLong(2, coder_id);
        psDel.setLong(3, problem_id);
        psDel.setInt(4, submission_number);
        psDel.setInt(5, example);
        psDel.executeUpdate();

        psIns.clearParameters();
        psIns.setLong(1, round_id); // round_id
        psIns.setLong(2, coder_id); // coder_id
        psIns.setLong(3, problem_id); // problem_id
        psIns.setDouble(4, rs.getDouble("points")); // final_points
        psIns.setInt(5, rs.getInt("status_id")); // status_id
        psIns.setInt(6, rs.getInt("language_id")); // language_id
        psIns.setLong(7, rs.getLong("open_time")); // open_time
        psIns.setInt(8, rs.getInt(14)); // submission_number
        if (Arrays.equals(getBytes(rs, 9), "".getBytes()))
          setBytes(psIns, 9, getBytes(rs, 13)); // use compilation_text
        else
          setBytes(psIns, 9, getBytes(rs, 9)); // use submission_text
        psIns.setLong(10, rs.getLong(10)); // submit_time
        psIns.setDouble(11, rs.getDouble(11)); // submission_points
        psIns.setString(12, rs.getString(12)); // status_desc
        psIns.setInt(13, last_submission); // last_submission
        psIns.setInt(14, example);

        retVal = psIns.executeUpdate();
        count += retVal;
        if (retVal != 1) {
          throw new SQLException("TCLoadRound: Insert for coder_id " + coder_id + ", round_id " + round_id
              + ", problem_id " + problem_id + " modified " + retVal + " rows, not one.");
        }

        logger.info("... loaded " + count + " rows for problem_submission table ...");
      }

      logger.info("problem_submission records copied = " + count);
    } catch (Exception e) {
      logger.error("load of problem_submission failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  public void loadSystemTestCase(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psDel = null;
    ResultSet rs = null;
    StringBuffer query = null;

    logger.info("=== start: loadSystemTestCase ===");
    try {
      query = new StringBuffer(100);
      query.append("SELECT stc.test_case_id "); // 1
      query.append("       ,comp.problem_id "); // 2
      query.append("       ,stc.args "); // 3
      query.append("       ,stc.expected_result "); // 4
      query.append("       ,CURRENT "); // 5
      query.append("       ,stc.example_flag ");
      query.append("       ,stc.system_flag ");
      query.append("  FROM system_test_case stc, component comp ");
      query.append(" WHERE comp.component_id in (SELECT component_id FROM round_component WHERE round_id = ?)");
      query.append(" AND comp.component_id = stc.component_id");
      // load only active tests
      query.append(" AND stc.status = " + ACTIVE_TEST_STATUS);
      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO system_test_case ");
      query.append("      (test_case_id "); // 1
      query.append("       ,problem_id "); // 2
      query.append("       ,args "); // 3
      query.append("       ,expected_result "); // 4
      query.append("       ,modify_date "); // 5
      query.append("       ,example_flag"); // 6
      query.append("       ,system_flag)"); // 7
      query.append("VALUES ( ");
      query.append("?,?,?,?,?,?,?)"); // 7 total values
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("DELETE FROM system_test_case ");
      query.append(" WHERE test_case_id = ? ");
      query.append("   AND problem_id = ?");
      psDel = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        int test_case_id = rs.getInt(1);
        int problem_id = rs.getInt(2);

        psDel.clearParameters();
        psDel.setInt(1, test_case_id);
        psDel.setInt(2, problem_id);
        psDel.executeUpdate();

        psIns.clearParameters();
        psIns.setInt(1, rs.getInt(1)); // test_case_id
        psIns.setInt(2, rs.getInt(2)); // problem_id
        setBytes(psIns, 3, getBlobObject(rs, 3)); // args
        setBytes(psIns, 4, getBlobObject(rs, 4)); // expected_result
        psIns.setTimestamp(5, rs.getTimestamp(5)); // modify_date
        psIns.setInt(6, rs.getInt("example_flag"));
        psIns.setInt(7, rs.getInt("system_flag"));

        retVal = psIns.executeUpdate();
        count += retVal;
        if (retVal != 1) {
          throw new SQLException("TCLoadRound: Insert for test_case_id " + test_case_id + ", problem_id " + problem_id
              + " modified more than one row.");
        }

        logger.info("... loaded " + count + " rows for system_test_case table ...");
      }

      logger.info("system_test_case records copied = " + count);
    } catch (Exception e) {
      logger.error("load of system_test_case failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  public void loadSystemTestResult(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psDel = null;
    ResultSet rs = null;
    StringBuffer query = null;

    logger.info("=== start: loadSystemTestResult ===");
    try {
      query = new StringBuffer(100);
      query.append("SELECT str.coder_id ");
      query.append(",str.round_id ");
      query.append(",comp.problem_id ");
      query.append(",str.test_case_id ");
      query.append(",str.submission_number ");
      query.append(",str.example ");
      query.append(",str.processing_time ");
      query.append(",str.timestamp ");
      query.append(",str.fatal_errors ");
      query.append(",str.score ");
      query.append(",str.test_action ");
      query.append("FROM long_system_test_result str, component comp ");
      query.append("WHERE str.round_id = ? ");
      query.append("AND comp.component_id = str.component_id ");
      query.append("AND str.coder_id NOT IN ");
      query.append("       (SELECT ugx.login_id ");
      query.append("          FROM user_group_xref ugx ");
      query.append("         WHERE ugx.group_id = 2000115) ");
      query.append(" AND str.coder_id NOT IN ");
      query.append("       (SELECT user_id ");
      query.append("          FROM group_user");
      query.append("         WHERE group_id = 13)");

      psSel = oltpConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO long_system_test_result ");
      query.append("      (coder_id ");
      query.append("       ,round_id ");
      query.append("       ,problem_id ");
      query.append("       ,test_case_id ");
      query.append("       ,submission_number");
      query.append("       ,example ");
      query.append("       ,processing_time ");
      query.append("       ,timestamp ");
      query.append("       ,fatal_errors ");
      query.append("       ,score ");
      query.append("       ,test_action_id ");
      query.append("       ,fatal_error_ind) ");
      query.append("VALUES (");
      query.append("?,?,?,?,?,?,?,?,?,?,?,?)"); // 12 values
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("DELETE FROM long_system_test_result ");
      query.append(" WHERE coder_id = ? ");
      query.append("   AND round_id = ? ");
      query.append("   AND problem_id = ? ");
      query.append("   AND test_case_id = ?");
      query.append("   and submission_number = ?");
      query.append("   and example = ?");
      psDel = dwConn.prepareStatement(query.toString());

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        long coder_id = rs.getLong("coder_id");
        long round_id = rs.getLong("round_id");
        long problem_id = rs.getLong("problem_id");
        long test_case_id = rs.getLong("test_case_id");
        int subNum = rs.getInt("submission_number");
        int example = rs.getInt("example");

        psDel.clearParameters();
        psDel.setLong(1, coder_id);
        psDel.setLong(2, round_id);
        psDel.setLong(3, problem_id);
        psDel.setLong(4, test_case_id);
        psDel.setInt(5, subNum);
        psDel.setInt(6, example);
        psDel.executeUpdate();

        psIns.clearParameters();
        psIns.setLong(1, coder_id);
        psIns.setLong(2, round_id);
        psIns.setLong(3, problem_id);
        psIns.setLong(4, test_case_id);
        psIns.setInt(5, subNum);
        psIns.setLong(6, example);
        psIns.setFloat(7, rs.getLong("processing_time"));
        psIns.setTimestamp(8, rs.getTimestamp("timestamp"));
        psIns.setBytes(9, rs.getBytes("fatal_errors"));
        psIns.setDouble(10, rs.getDouble("score"));
        psIns.setInt(11, rs.getInt("test_action"));
        psIns.setInt(12, rs.getBytes("fatal_errors") == null ? 0 : 1);

        retVal = psIns.executeUpdate();
        count += retVal;
        if (retVal != 1) {
          throw new SQLException("TCLoadRound: Insert for coder_id " + coder_id + ", round_id " + round_id
              + ", problem_id " + problem_id + ", test_case_id " + test_case_id + " subnum " + subNum + " example: "
              + example + " modified " + retVal + " rows, not one.");
        }

        logger.info("... loaded " + count + " rows for long_system_test_result table ...");
      }

      logger.info("long_system_test_result records copied = " + count);
    } catch (Exception e) {
      logger.error("load of long_system_test_result failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  public void loadResult(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int retVal = 0;
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psDel = null;
    PreparedStatement psNumRatings = null;
    ResultSet rs = null;
    StringBuffer query = null;
    int round_id = 0;
    int coder_id = 0;

    logger.info("=== start: loadResult " + roundId + " ===");
    try {
      query = new StringBuffer(100);

      query.append("select rr.coder_id ");
      query.append("     , rr.round_id ");
      query.append("     , rr.placed ");
      query.append("     , rr.point_total ");
      query.append("     , rr.system_point_total ");
      query.append("     , cs.submission_number ");
      query.append("     , rr.attended");
      query.append("     , rr.old_rating");
      query.append("     , rr.new_rating");
      query.append("     , rr.old_vol");
      query.append("     , rr.new_vol");
      query.append("     , rr.rated_ind");
      query.append("     , rr.advanced");
      query.append("  from long_comp_result rr ");
      query.append("     , long_component_state cs ");
      query.append(" where rr.round_id = ? ");
      query.append("   and cs.round_id = rr.round_id ");
      query.append("   and cs.coder_id = rr.coder_id ");
      query.append("   AND NOT EXISTS ");
      query.append("       (SELECT 'pops' ");
      query.append("          FROM user_group_xref ugx ");
      query.append("         WHERE ugx.login_id=rr.coder_id ");
      query.append("           AND ugx.group_id = 2000115)");
      query.append("   AND NOT EXISTS ");
      query.append("       (SELECT 'pops' ");
      query.append("          FROM group_user gu ");
      query.append("         WHERE gu.user_id = rr.coder_id ");
      query.append("           AND gu.group_id = 13)");
      query.append(" ORDER BY point_total desc");

      psSel = oltpConn.prepareStatement(query.toString());

      // Create a map with the number of ratings that each coder has previous to the
      // round being loaded
      Map<Integer, Integer> numRatings = new HashMap<Integer, Integer>();
      query = new StringBuffer(100);
      query.append(" select  lcr.coder_id, max(num_ratings) as num");
      query.append(" from round r1 ");
      query.append(" , round r2 ");
      query.append(" , round_type_lu rt1 ");
      query.append(" , round_type_lu rt2 ");
      query.append(" , long_comp_result lcr ");
      query.append(" where r2.rating_order < r1.rating_order ");
      query.append(" and r2.round_id = lcr.round_id ");
      query.append(" and r1.round_id = ? ");
      query.append(" and r1.round_type_id = rt1.round_type_id ");
      query.append(" and r2.round_type_id = rt2.round_type_id ");
      query.append(" and rt1.algo_rating_type_id = rt2.algo_rating_type_id ");
      query.append(" group by coder_id ");
      psNumRatings = dwConn.prepareStatement(query.toString());
      psNumRatings.setInt(1, roundId);
      rs = psNumRatings.executeQuery();

      while (rs.next()) {
        numRatings.put(rs.getInt("coder_id"), rs.getInt("num"));
      }

      dbHelper.closeResultSet(rs);

      query = new StringBuffer(100);
      query.append("INSERT INTO long_comp_result ");
      query.append("      (round_id "); // 1
      query.append("       ,coder_id "); // 2
      query.append("       ,placed"); // 3
      query.append("       ,point_total "); // 4
      query.append("       ,system_point_total "); // 5
      query.append("       ,num_submissions"); // 6
      query.append("       ,attended"); // 7
      query.append("       ,old_rating"); // 8
      query.append("       ,new_rating"); // 9
      query.append("       ,old_vol"); // 10
      query.append("       ,new_vol"); // 11
      query.append("       ,rated_ind"); // 12
      query.append("       ,advanced"); // 13
      query.append("       ,old_rating_id"); // 14
      query.append("       ,new_rating_id"); // 15
      query.append("       ,provisional_placed"); // 16
      query.append("       ,num_ratings)"); // 17

      query.append("VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      psIns = dwConn.prepareStatement(query.toString());

      // provisional rank
      int provRank = 0;
      int provRankNoTie = 0;
      double provScore = -1;

      // On to the load
      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        round_id = rs.getInt("round_id");
        coder_id = rs.getInt("coder_id");

        psIns.clearParameters();
        psIns.setLong(1, round_id);
        psIns.setLong(2, coder_id);
        psIns.setObject(3, rs.getObject("placed"));
        psIns.setObject(4, rs.getObject("point_total"));
        psIns.setObject(5, rs.getObject("system_point_total"));
        psIns.setObject(6, rs.getObject("submission_number"));
        psIns.setString(7, rs.getString("attended"));
        psIns.setObject(8, rs.getObject("old_rating"));
        psIns.setObject(9, rs.getObject("new_rating"));
        psIns.setObject(10, rs.getObject("old_vol"));
        psIns.setObject(11, rs.getObject("new_vol"));
        psIns.setInt(12, rs.getInt("rated_ind"));
        psIns.setString(13, rs.getString("advanced"));
        // we can just use the rating because the id's and ratings match up. may not be
        // the case one day.
        psIns.setInt(14, rs.getInt("old_rating") == 0 ? -2 : rs.getInt("old_rating"));
        psIns.setInt(15, rs.getInt("new_rating") == 0 ? -2 : rs.getInt("new_rating"));

        if ("Y".equalsIgnoreCase(rs.getString("attended"))) {
          provRankNoTie++;
          if (provScore != rs.getDouble("point_total")) {
            provRank = provRankNoTie;
          }
          provScore = rs.getDouble("point_total");

          psIns.setInt(16, provRank);
        } else {
          psIns.setNull(16, Types.INTEGER);
        }

        int nr = numRatings.get(coder_id) == null ? 0 : numRatings.get(coder_id);
        if (rs.getInt("rated_ind") == 1)
          nr++;
        psIns.setInt(17, nr);

        retVal = psIns.executeUpdate();
        count += retVal;
        if (retVal != 1) {
          throw new SQLException("TCLoadRound: Insert for coder_id " + coder_id + ", round_id " + round_id
              + " modified " + retVal + " rows, not one.");
        }

        logger.info("... loaded " + count + " rows for long_comp_result table ...");
      }

      logger.info("long_comp_result records copied = " + count);
    } catch (Exception e) {
      logger.error("load of long_comp_result failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
      dbHelper.closeStatement(psNumRatings);
    }
  }

  public void loadRating(Connection oltpConn, Connection dwConn, int roundId) throws Exception {
    int count = 0;
    int retVal = 0;
    PreparedStatement psSel = null;
    PreparedStatement psRatedRound = null;
    PreparedStatement psSelNumCompetitions = null;
    PreparedStatement psSelRatedRounds = null;
    PreparedStatement psSelMinMaxRatings = null;
    PreparedStatement psUpd = null;
    ResultSet rs = null;
    ResultSet rs2 = null;
    StringBuffer query = null;

    logger.info("=== start: loadRating ===");
    try {
      query = new StringBuffer(100);
      query.append(" SELECT 1 FROM round");
      query.append(" WHERE round_id = " + roundId);
      query.append(" AND rated_ind = 1");
      psRatedRound = oltpConn.prepareStatement(query.toString());
      rs = psRatedRound.executeQuery();

      if (!rs.next()) {
        logger.info("not loading rating, since the round is not rated");
        return;
      }

      // Get all the coders that participated in this round
      query = new StringBuffer(100);
      query.append("SELECT lcr.coder_id "); // 1
      query.append("  FROM long_comp_result lcr ");
      query.append(" WHERE lcr.round_id = ? ");
      query.append("   AND lcr.attended = 'Y' ");
      query.append("   AND lcr.rated_ind = 1");
      query.append("   AND NOT EXISTS ");
      query.append("       (SELECT 'pops' ");
      query.append("          FROM user_group_xref ugx ");
      query.append("         WHERE ugx.login_id= lcr.coder_id ");
      query.append("           AND ugx.group_id = 2000115)");
      query.append("   AND NOT EXISTS ");
      query.append("       (SELECT 'pops' ");
      query.append("          FROM group_user gu ");
      query.append("         WHERE gu.user_id = lcr.coder_id ");
      query.append("           AND gu.group_id = 13)");

      psSel = oltpConn.prepareStatement(query.toString());

      // Select min and max ratings
      query = new StringBuffer(100);
      query.append("SELECT min(new_rating), ");
      query.append("  max(new_rating) ");
      query.append(" FROM long_comp_result ");
      query.append(" WHERE coder_id = ? ");
      query.append(" AND attended = 'Y' ");
      query.append(" AND rated_ind = 1 ");
      query.append(" AND new_rating > 0 ");
      psSelMinMaxRatings = dwConn.prepareStatement(query.toString());

      // No need to filter admins here as they have already been filtered from
      // the DW rating table
      query = new StringBuffer(100);
      query.append("SELECT first_rated_round_id "); // 1
      query.append("       ,last_rated_round_id "); // 2
      query.append("  FROM algo_rating ");
      query.append(" WHERE coder_id = ? ");
      query.append(" AND algo_rating_type_id = ? ");
      psSelRatedRounds = dwConn.prepareStatement(query.toString());

      // No need to filter admins here as they have already been filtered from
      // the DW rating table
      query = new StringBuffer(100);
      query.append("SELECT count(*) ");
      query.append(" FROM long_comp_result ");
      query.append(" WHERE coder_id =? ");
      query.append(" AND attended='Y' ");
      psSelNumCompetitions = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("UPDATE algo_rating ");
      query.append("   SET first_rated_round_id = ? "); // 1
      query.append("       ,last_rated_round_id = ? "); // 2
      query.append("       ,lowest_rating = ? "); // 3
      query.append("       ,highest_rating = ? "); // 4
      query.append("       ,num_competitions = ? "); // 5
      query.append(" WHERE coder_id = ?"); // 6
      query.append("   AND algo_rating_type_id = ? ");
      psUpd = dwConn.prepareStatement(query.toString());

      psSel.setInt(1, roundId);
      rs = psSel.executeQuery();

      while (rs.next()) {
        int coder_id = rs.getInt(1);

        int num_competitions = -1;
        int first_rated_round_id = -1;
        int last_rated_round_id = -1;
        int lowest_rating = -1;
        int highest_rating = -1;

        // Get the existing first and last rated round ids in case they are
        // already there.
        psSelRatedRounds.clearParameters();
        psSelRatedRounds.setInt(1, coder_id);
        psSelRatedRounds.setInt(2, MARATHON_RATING_TYPE_ID);
        rs2 = psSelRatedRounds.executeQuery();
        if (rs2.next()) {
          if (rs2.getString(1) != null)
            first_rated_round_id = rs2.getInt(1);
          if (rs2.getString(2) != null)
            last_rated_round_id = rs2.getInt(2);
        }

        dbHelper.closeResultSet(rs2);

        // Get the number of competitions
        psSelNumCompetitions.clearParameters();
        psSelNumCompetitions.setInt(1, coder_id);
        rs2 = psSelNumCompetitions.executeQuery();
        if (rs2.next()) {
          num_competitions = rs2.getInt(1);
        }

        dbHelper.closeResultSet(rs2);

        // Get the new min/max ratings to see if we
        psSelMinMaxRatings.clearParameters();
        psSelMinMaxRatings.setInt(1, coder_id);
        rs2 = psSelMinMaxRatings.executeQuery();
        if (rs2.next()) {
          lowest_rating = rs2.getInt(1);
          highest_rating = rs2.getInt(2);
        }

        dbHelper.closeResultSet(rs2);

        // Check to see if any of the round ids need to be updated to be this
        // round id. If the round we are loading is prior to the first rated
        // round (or it isn't set) we set this round as the first rated round.
        // If the round we are loading is greater than the last rated round
        // (or it isn't set), we set this round as the last rated round
        if (first_rated_round_id == -1 ||
            getRoundStart(oltpConn, roundId).compareTo(getRoundStart(oltpConn, first_rated_round_id)) < 0)
          first_rated_round_id = roundId;

        if (last_rated_round_id == -1 ||
            getRoundStart(oltpConn, roundId).compareTo(getRoundStart(oltpConn, last_rated_round_id)) > 0)
          last_rated_round_id = roundId;

        // Finally, do update
        psUpd.clearParameters();
        psUpd.setInt(1, first_rated_round_id); // first_rated_round_id
        psUpd.setInt(2, last_rated_round_id); // last_rated_round_id
        psUpd.setInt(3, lowest_rating); // lowest_rating
        psUpd.setInt(4, highest_rating); // highest_rating
        psUpd.setInt(5, num_competitions); // num_competitions
        psUpd.setInt(6, coder_id); // coder_id
        psUpd.setInt(7, MARATHON_RATING_TYPE_ID); // algo_rating_type_id

        retVal = psUpd.executeUpdate();
        count = count + retVal;
        if (retVal != 1) {
          throw new SQLException(
              "TCLoadLong: Insert for coder_id " + coder_id + " modified " + retVal + " rows, not one.");
        }

        logger.info("... loaded " + count + " rows for rating table ...");
      }

      logger.info("rating records updated = " + count);
    } catch (Exception e) {
      logger.error("load of algo_rating failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeResultSet(rs2);
      dbHelper.closeStatement(psRatedRound);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psSelNumCompetitions);
      dbHelper.closeStatement(psSelRatedRounds);
      dbHelper.closeStatement(psSelMinMaxRatings);
      dbHelper.closeStatement(psUpd);
    }
  }

  public void copyHistory(Connection dwConn, int prevRoundId, int roundId) throws SQLException {
    logger.debug("copy algo_rating_history from round " + prevRoundId + " to " + roundId);
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    ResultSet rs = null;

    StringBuffer query = new StringBuffer(100);
    query.append("SELECT coder_id, algo_rating_type_id, rating,vol, num_ratings ");
    query.append(" from algo_rating_history where round_id = ?");
    psSel = dwConn.prepareStatement(query.toString());
    psSel.setInt(1, prevRoundId);

    query = new StringBuffer(100);
    query.append("INSERT INTO algo_rating_history(coder_id, round_id, algo_rating_type_id, rating,vol, num_ratings) ");
    query.append("VALUES (?,?,?,?,?,?)");
    psIns = dwConn.prepareStatement(query.toString());

    int count = 0;
    try {
      rs = psSel.executeQuery();

      while (rs.next()) {
        psIns.clearParameters();
        psIns.setInt(1, rs.getInt("coder_id"));
        psIns.setInt(2, roundId);
        psIns.setInt(3, rs.getInt("algo_rating_type_id"));
        psIns.setInt(4, rs.getInt("rating"));
        psIns.setInt(5, rs.getInt("vol"));
        psIns.setInt(6, rs.getInt("num_ratings"));
        psIns.executeUpdate();

        count++;
        logger.info("... loaded " + count + " rows for ratings table ...");
      }

      logger.info("algo_history_rating copied from " + prevRoundId + " to " + roundId + ": " + count);

    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
    }
  }

  public Date getRoundStart(Connection dbConnection, int roundId) throws SQLException {
    StringBuffer query = null;
    if (fRoundStartHT.get(roundId) != null)
      return fRoundStartHT.get(roundId);

    query = new StringBuffer(100);
    query.append("SELECT c.start_date ");
    query.append("  FROM contest c, round r ");
    query.append(" WHERE r.round_id = ? ");
    query.append("   AND c.contest_id = r.contest_id");
    PreparedStatement pSel = dbConnection.prepareStatement(query.toString());

    pSel.setInt(1, roundId);
    ResultSet rs = pSel.executeQuery();

    if (rs.next()) {
      java.sql.Date date = rs.getDate(1);
      fRoundStartHT.put(roundId, date);
      return date;
    } else {
      throw new SQLException("unable to determine start for " + roundId);
    }
  }

  public boolean isRated(Connection oltpConn, long roundId) throws Exception {
    PreparedStatement psRatedRound = null;
    ResultSet rs = null;
    StringBuffer query = null;
    try {
      query = new StringBuffer(100);
      query.append(" SELECT 1 FROM round");
      query.append(" WHERE round_id = " + roundId);
      query.append(" AND rated_ind = 1");
      psRatedRound = oltpConn.prepareStatement(query.toString());
      rs = psRatedRound.executeQuery();

      return rs.next();
    } catch (Exception e) {
      logger.error("could not check if round " + roundId + " was rated or not");
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psRatedRound);
    }
    return false;
  }

  public int lookupCalendarId(java.sql.Timestamp date, Connection dbConnection)
      throws SQLException {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    StringBuffer query = null;

    int year = cal.get(Calendar.YEAR);

    // The month is based on 0 for January so we need to add 1 to get
    // the right lookup value
    int month_of_year = cal.get(Calendar.MONTH) + 1;

    int day_of_month = cal.get(Calendar.DAY_OF_MONTH);

    PreparedStatement psSel = null;
    ResultSet rs = null;

    query = new StringBuffer(100);
    query.append("SELECT calendar_id ");
    query.append("  FROM calendar ");
    query.append(" WHERE year = ? ");
    query.append("   AND month_numeric = ? ");
    query.append("   AND day_of_month = ? ");
    psSel = dbConnection.prepareStatement(query.toString());

    psSel.setInt(1, year);
    psSel.setInt(2, month_of_year);
    psSel.setInt(3, day_of_month);

    try {
      rs = psSel.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      } else {
        throw new SQLException("Unable to locate calendar_id for " + date.toString());
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
    }
  }

  public int lookupTimeId(java.sql.Timestamp date, Connection dbConnection) throws SQLException {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    StringBuffer query = null;

    int minute = cal.get(Calendar.MINUTE);
    int hour = cal.get(Calendar.HOUR_OF_DAY);

    PreparedStatement psSel = null;
    ResultSet rs = null;

    query = new StringBuffer(100);
    query.append("SELECT time_id ");
    query.append("  FROM time ");
    query.append(" WHERE minute = ? ");
    query.append("   AND hour_24 = ? ");
    psSel = dbConnection.prepareStatement(query.toString());

    psSel.setInt(1, minute);
    psSel.setInt(2, hour);

    try {
      rs = psSel.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      } else {
        throw new SQLException("Unable to locate time_id for " + date.toString());
      }
    } catch (SQLException sqle) {
      throw sqle;
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
    }
  }

  public byte[] getBlobObject(ResultSet rs, int index) throws Exception {
    Object retVal = null;
    ObjectInputStream ois = null;
    try {

      InputStream is = rs.getBinaryStream(index);
      if (is != null)
        ois = new ObjectInputStream(is);
      if (ois != null) {
        retVal = ois.readObject();
      }

      if (ois != null) {
        ois.close();
      }

      return serializeBlobObject(retVal);

    } catch (Exception e) {
      logger.error("error in getBlobObject");
      logger.error(e.getMessage());
      logger.error("", e);
      throw e;
    }
  }

  public void setBytes(PreparedStatement pstmt, int index, byte[] value)
      throws SQLException {
    if (value != null)
      pstmt.setBytes(index, value);
    else
      pstmt.setNull(index, java.sql.Types.BINARY);
  }

  public byte[] getBytes(ResultSet rs, int index) throws SQLException {
    try {
      String text = getTextString(rs, index);
      if (text == null)
        throw new IllegalArgumentException("DBMS.serializeTextString:ERROR text is NULL");

      return text.getBytes();
    } catch (Exception e) {
      logger.error("error in getBytes");
      logger.error(e.getMessage());
      logger.error("", e);
      throw e;
    }
  }

  public String getTextString(ResultSet rs, int column) throws SQLException {
    if (rs == null)
      throw new IllegalArgumentException("DBMS.getTextString:ERROR ResultSet is NULL");
    try {
      byte[] bytes = rs.getBytes(column);
      String text = "";

      if (bytes == null) {
        text = "";
      } else {
        text = new String(bytes);
      }

      return new String(text);
    } catch (SQLException e) {
      logger.error("error in getTextString");
      logger.error(e.getMessage());
      logger.error("", e);
      throw e;
    }
  }

  public byte[] serializeBlobObject(Object obj) {
    if (obj == null)
      throw new IllegalArgumentException("DBMS.serializeBlobObject:ERROR obj is NULL");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] bytes = null;

    try {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      bytes = baos.toByteArray();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return bytes;
  }

  public void clearHistory(Connection dbConnection, int roundId) throws Exception {
    PreparedStatement psDel = null;

    psDel = dbConnection.prepareStatement("delete from algo_rating_history where round_id=?");
    psDel.setInt(1, roundId);
    psDel.executeUpdate();
  }

  public void loadHistory(Connection dwConn, int roundId) throws SQLException {

    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psUpd = null;
    ResultSet rs = null;

    StringBuffer query = new StringBuffer(100);
    query.append("SELECT coder_id, coder_id, new_rating, new_vol ");
    query.append("FROM long_comp_result ");
    query.append("WHERE round_id = ? ");
    query.append("AND rated_ind=1 ");
    psSel = dwConn.prepareStatement(query.toString());
    psSel.setInt(1, roundId);

    query = new StringBuffer(100);
    query.append("UPDATE algo_rating_history SET rating=?, vol=?, num_ratings=num_ratings+1 ");
    query.append("WHERE round_id = ? AND coder_id=?");
    psUpd = dwConn.prepareStatement(query.toString());

    query = new StringBuffer(100);
    query.append("INSERT INTO algo_rating_history(coder_id, round_id, algo_rating_type_id, rating,vol, num_ratings) ");
    query.append("VALUES (?,?,3,?,?,1)");
    psIns = dwConn.prepareStatement(query.toString());

    int count = 0;
    try {
      rs = psSel.executeQuery();

      while (rs.next()) {
        psUpd.clearParameters();
        psUpd.setInt(1, rs.getInt("new_rating"));
        psUpd.setInt(2, rs.getInt("new_vol"));
        psUpd.setInt(3, roundId);
        psUpd.setInt(4, rs.getInt("coder_id"));
        int retVal = psUpd.executeUpdate();
        if (retVal == 0) {
          psIns.clearParameters();
          psIns.setInt(1, rs.getInt("coder_id"));
          psIns.setInt(2, roundId);
          psIns.setInt(3, rs.getInt("new_rating"));
          psIns.setInt(4, rs.getInt("new_vol"));
          psIns.executeUpdate();
        }


        count++;
        logger.info("... loaded " + count + " rows for ratings inserted ...");
      }

      logger.info("algo_history_rating inserted from long_comp_result: " + count);

    } catch (Exception e) {
      logger.error("error in loadHistory");
      logger.error(e.getMessage());
      logger.error("", e);
      throw e;
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psUpd);
    }
  }

  public Integer getPreviousRound(Connection dbConnection, int roundId) throws Exception {
    PreparedStatement ps = null;
    ResultSet rs = null;

    StringBuffer query = new StringBuffer(100);
    query.append("select r1.round_id from round r1, round_type_lu rt1, round r2 ");
    query.append("where r1.rating_order = r2.rating_order - 1  ");
    query.append("and rt1.round_type_id = r1.round_type_id ");
    query.append("and rt1.algo_rating_type_id = 3 ");
    query.append("and r2.round_id = ? ");

    Integer previous = null;
    try {
      ps = dbConnection.prepareStatement(query.toString());
      ps.setInt(1, roundId);
      rs = ps.executeQuery();
      if (rs.next()) {
        previous = rs.getInt("round_id");
      }
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(ps);
    }
    return previous;
  }

  public void loadStreaks(Connection dwConn) throws Exception {
    int count = 0;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    PreparedStatement psDel = null;
    ResultSet rs = null;
    StringBuffer query = null;

    try {
      // Get all the coders that participated in this round
      query = new StringBuffer(100);
      query.append(" select r.round_id, coder_id, placed, new_rating,round_type_id, r.calendar_id ");
      query.append(" from long_comp_result lcr, ");
      query.append(" round r ");
      query.append(" where lcr.round_id = r.round_id ");
      query.append(" and r.round_type_id in (13,19) ");
      query.append(" and lcr.attended='Y' ");
      query.append(" and lcr.rated_ind = 1 ");
      query.append(" order by coder_id, r.calendar_id ");
      psSel = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("INSERT INTO streak ");
      query.append("      (coder_id "); // 1
      query.append("       ,streak_type_id "); // 2
      query.append("       ,start_round_id "); // 3
      query.append("       ,end_round_id "); // 4
      query.append("       ,length "); // 5
      query.append("       ,is_current) "); // 6
      query.append("VALUES (?,?,?,?,?,?)"); // 6 total values
      psIns = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append("DELETE FROM streak WHERE streak_type_id in (" + AlgoStreak.MARATHON_RATING_INCREASE + ", "
          + AlgoStreak.MARATHON_RATING_INCREASE_ALL + ", " +
          AlgoStreak.MARATHON_CONSECUTIVE_TOP_5 + "," + AlgoStreak.MARATHON_CONSECUTIVE_TOP_10 + ")");
      psDel = dwConn.prepareStatement(query.toString());

      psDel.executeUpdate();
      rs = psSel.executeQuery();

      AlgoStreak streaks[] = new AlgoStreak[] {
          new RatingIncrease(),
          new RatingIncreaseAll(),
          new Top5Streak(),
          new Top10Streak() };

      int roundId = 0;
      int coderId = 0;
      int placed = 0;
      int newRating = 0;
      int roundTypeId = 0;

      boolean hasNext = true;
      while (hasNext) {
        hasNext = rs.next();

        if (hasNext) {
          roundId = rs.getInt("round_id");
          coderId = rs.getInt("coder_id");
          placed = rs.getInt("placed");
          newRating = rs.getInt("new_rating");
          roundTypeId = rs.getInt("round_type_id");
        }

        for (int k = 0; k < streaks.length; k++) {
          AlgoStreak.StreakRow sr = hasNext ? streaks[k].add(coderId, roundId, placed, newRating, roundTypeId)
              : streaks[k].flush();

          if (sr != null) {
            psIns.setInt(1, sr.getCoderId());
            psIns.setInt(2, sr.getStreakType());
            psIns.setInt(3, sr.getStartRoundId());
            psIns.setInt(4, sr.getEndRoundId());
            psIns.setInt(5, sr.getLength());
            psIns.setInt(6, sr.isCurrent() ? 1 : 0);

            psIns.executeUpdate();
            count++;

            logger.info("... loaded " + count + " streak ...");
          }
        }
      }
      logger.info("loaded " + count + " records for streaks");
    } catch (Exception e) {
      logger.error("load of 'streak' table failed ");
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  public void setLastUpdateTime(Connection dwConn, java.sql.Timestamp fStartTime) throws Exception {
    PreparedStatement psUpd = null;
    StringBuffer query = null;

    try {
      int retVal = 0;
      query = new StringBuffer(100);
      query.append("INSERT INTO update_log ");
      query.append("      (log_id "); // 1
      query.append("       ,calendar_id "); // 2
      query.append("       ,timestamp  "); // 3
      query.append("       ,log_type_id) "); // 4
      query.append("VALUES (0, ?, ?, " + ROUND_LOG_TYPE + ")");
      psUpd = dwConn.prepareStatement(query.toString());

      int calendar_id = lookupCalendarId(fStartTime, dwConn);
      psUpd.setInt(1, calendar_id);
      psUpd.setTimestamp(2, fStartTime);

      retVal = psUpd.executeUpdate();
      if (retVal != 1) {
        throw new SQLException("SetLastUpdateTime updated " + retVal +
            " rows, not just one.");
      }
    } catch (Exception e) {
      logger.error("failed to set last log time ");
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeStatement(psUpd);
    }
  }

  class RatingIncreaseAll extends AlgoStreak {
    private int currentRating = -1;

    protected RatingIncreaseAll(int streakTypeId) {
      super(streakTypeId);
    }

    public RatingIncreaseAll() {
      super(MARATHON_RATING_INCREASE_ALL);
    }

    @Override
    protected boolean addToStreak(int placed, int rating) {
      boolean accept = currentRating < 0 ? false : rating > currentRating;
      currentRating = rating;
      return accept;
    }

    protected void reset() {
      currentRating = -1;
    }
  }

  /**
   * Streak of rating increases for non tournament rounds.
   *
   * @author Cucu
   */
  class RatingIncrease extends RatingIncreaseAll {
    public RatingIncrease() {
      super(MARATHON_RATING_INCREASE);
    }

    /**
     * Skip tournament rounds
     */
    protected boolean skipRound(int roundTypeId) {
      return roundTypeId == ROUND_TYPE_MARATHON_TOURNAMENT;
    }
  }

  /**
   * Streak of top 5 placements.
   *
   * @author Cucu
   */
  class Top5Streak extends AlgoStreak {
    public Top5Streak() {
      super(MARATHON_CONSECUTIVE_TOP_5);
    }

    @Override
    protected boolean addToStreak(int placed, int rating) {
      return placed > 0 && placed <= 5;
    }
  }

  /**
   * Streak of top 10 placements.
   *
   * @author Cucu
   */
  class Top10Streak extends AlgoStreak {
    public Top10Streak() {
      super(MARATHON_CONSECUTIVE_TOP_10);
    }

    @Override
    protected boolean addToStreak(int placed, int rating) {
      return placed > 0 && placed <= 10;
    }
  }

}
