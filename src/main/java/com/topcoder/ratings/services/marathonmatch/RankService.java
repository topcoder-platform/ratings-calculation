package com.topcoder.ratings.services.marathonmatch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.topcoder.ratings.database.DBHelper;

public class RankService {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  final int MARATHON_RATING_TYPE_ID = 3;
  final int OVERALL_RATING_RANK_TYPE_ID = 1;
  final int ACTIVE_RATING_RANK_TYPE_ID = 2;

  DBHelper dbHelper = new DBHelper();

  public List<CoderRating> getRatingsForRound(Connection dwConn, int roundId, int algoType) throws Exception {
    StringBuffer query = null;
    PreparedStatement psSel = null;
    ResultSet rs = null;
    List<CoderRating> ret = null;

    try {
      query = new StringBuffer(1000);
      query.append("select r.coder_id ");
      query.append(" , r.rating ");
      query.append(" , cs.school_id ");
      query.append(" , c.coder_type_id ");
      query.append(" , c.comp_country_code as country_code ");
      query.append(" , c.state_code ");

      if (algoType == MARATHON_RATING_TYPE_ID) {
        query.append(" , case when exists (select '1'");
        query.append("                       from long_comp_result lcr ");
        query.append("                         , round r1 ");
        query.append("                         , calendar cal ");
        query.append("                         , round_type_lu  rt ");
        query.append("                       where lcr.round_id = r1.round_id ");
        query.append("                         and lcr.attended = 'Y' ");
        query.append("                         and r1.round_type_id = rt.round_type_id ");
        query.append("                         and rt.algo_rating_type_id = " + MARATHON_RATING_TYPE_ID);
        query.append("                         and lcr.rated_ind = 1  ");
        query.append("                         and r1.calendar_id = cal.calendar_id  ");
        query.append("                         and lcr.coder_id = r.coder_id ");
        query.append(
            "                         and cal.calendar_id <= (select calendar_id from round where round_id = r.round_id)  ");
        query.append(
            "                         and cal.date >= (select c2.date - interval(180) day(9) to day from round r2, calendar c2 ");
        query.append(
            "                             where r2.calendar_id = c2.calendar_id and r2.round_id = r.round_id)) ");
        query.append("                 then 1 else 0 end as active ");
      } else {
        query.append(" , case when exists (select '1'");
        query.append("                       from room_result rr");
        query.append("                          , round r1");
        query.append("                          , calendar cal");
        query.append("                          , round_type_lu  rt");
        query.append("                      where rr.round_id = r1.round_id");
        query.append("                        and rr.attended = 'Y'");
        query.append("                        and r1.round_type_id = rt.round_type_id");
        query.append("                        and rt.algo_rating_type_id = r.algo_rating_type_id ");
        query.append("                        and rr.rated_flag = 1 ");
        query.append("                        and r1.calendar_id = cal.calendar_id");
        query.append("                        and rr.coder_id = r.coder_id");
        query.append(
            "                        and cal.calendar_id <= (select calendar_id from round where round_id = r.round_id)");
        query.append(
            "                        and cal.date >= (select c2.date - interval(180) day(9) to day from round r2, calendar c2");
        query.append(
            "                                                  where r2.calendar_id = c2.calendar_id and r2.round_id = r.round_id))");
        query.append("                 then 1 else 0 end as active ");
      }

      query.append("  from algo_rating_history r ");
      query.append(" , outer current_school cs ");
      query.append(" , coder c ");
      query.append(" where r.coder_id = cs.coder_id");
      query.append(" and r.coder_id = c.coder_id ");
      query.append(" and r.algo_rating_type_id = " + algoType);
      query.append(" and r.num_ratings > 0 ");
      query.append(" and c.status = 'A' ");
      query.append(" and r.round_id = ?");

      psSel = dwConn.prepareStatement(query.toString());
      psSel.setInt(1, roundId);

      rs = psSel.executeQuery();
      ret = new ArrayList<CoderRating>();
      while (rs.next()) {
        // pros
        if (rs.getInt("coder_type_id") == 2) {
          ret.add(new CoderRating(rs.getLong("coder_id"), rs.getInt("rating"), 0, rs.getInt("active") == 1,
              rs.getString("country_code"), rs.getString("state_code")));
        } else {
          ret.add(new CoderRating(rs.getLong("coder_id"), rs.getInt("rating"), rs.getInt("school_id"),
              rs.getInt("active") == 1, rs.getString("country_code"), rs.getString("state_code")));
        }
      }
    } catch (Exception e) {
      logger.error("get list of current ratings failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
    }
    return ret;
  }

  public void loadRatingRank(Connection dwConn, int roundId, int rankType, int ratingType, List list) throws Exception {
    logger.debug("loadRatingRank called...");
    StringBuffer query = null;
    PreparedStatement psDel = null;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    ResultSet rs = null;
    int count = 0;
    int coderCount = 0;

    try {

      query = new StringBuffer(100);
      query.append(" DELETE");
      query.append(" FROM coder_rank");
      query.append(" WHERE coder_rank_type_id = " + rankType);
      query.append(" AND algo_rating_type_id = " + ratingType);
      psDel = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" INSERT");
      query.append(" INTO coder_rank (coder_id, percentile, rank, coder_rank_type_id, algo_rating_type_id)");
      query.append(" VALUES (?, ?, ?, " + rankType + ", " + ratingType + ")");
      psIns = dwConn.prepareStatement(query.toString());

      /*
       * coder_rank table should be kept "up-to-date" so get the most recent stuff
       * from the rating table
       */
      // ratings = getCurrentCoderRatings(rankType == ACTIVE_RATING_RANK_TYPE_ID);

      ArrayList ratings = new ArrayList(list.size());
      CoderRating cr = null;
      for (int i = 0; i < list.size(); i++) {
        cr = (CoderRating) list.get(i);
        if ((rankType == ACTIVE_RATING_RANK_TYPE_ID && cr.isActive()) ||
            rankType != ACTIVE_RATING_RANK_TYPE_ID) {
          ratings.add(cr);
        }
      }
      Collections.sort(ratings);
      coderCount = ratings.size();

      // delete all the records for the overall rating rank type
      psDel.executeUpdate();

      int i = 0;
      int rating = 0;
      int rank = 0;
      int size = ratings.size();
      int tempRating = 0;
      long tempCoderId = 0;
      for (int j = 0; j < size; j++) {
        i++;
        tempRating = ((CoderRating) ratings.get(j)).getRating();
        tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
        if (tempRating != rating) {
          rating = tempRating;
          rank = i;
        }
        psIns.setLong(1, tempCoderId);
        psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
        psIns.setInt(3, rank);
        count += psIns.executeUpdate();

        logger.info("... loaded " + count + " overall rating rank ...");
      }
      logger.info("records loaded for overall rating rank load: " + count);

    } catch (Exception e) {
      logger.error("load of 'coder_rank' table failed for overall rating rank for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  public void loadRatingRankHistory(Connection dwConn, int roundId, int rankType, int ratingType, List list)
      throws Exception {
    logger.debug("loadRatingRankHistory called...");
    StringBuffer query = null;
    PreparedStatement psDel = null;
    PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    ResultSet rs = null;
    int count = 0;
    int coderCount = 0;
    List ratings = null;

    try {

      query = new StringBuffer(100);
      query.append(" DELETE");
      query.append(" FROM coder_rank_history");
      query.append(" WHERE coder_rank_type_id = " + rankType);
      query.append(" AND round_id = " + roundId);
      query.append(" AND algo_rating_type_id = " + ratingType);
      psDel = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" INSERT");
      query.append(
          " INTO coder_rank_history (coder_id, round_id, percentile, rank, coder_rank_type_id, algo_rating_type_id)");
      query.append(" VALUES (?, ?, ?, ?, " + rankType + "," + ratingType + ")");
      psIns = dwConn.prepareStatement(query.toString());

      if (rankType == ACTIVE_RATING_RANK_TYPE_ID) {
        ratings = new ArrayList();
        for (Iterator i = list.iterator(); i.hasNext();) {
          CoderRating rating = (CoderRating) i.next();
          if (rating.active) {
            ratings.add(rating);
          }
        }
      } else {
        ratings = list;
      }

      coderCount = ratings.size();
      Collections.sort(ratings);

      // delete all the recordsfor the rating rank type
      psDel.executeUpdate();

      int i = 0;
      int rating = 0;
      int rank = 0;
      int size = ratings.size();
      int tempRating = 0;
      long tempCoderId = 0;
      for (int j = 0; j < size; j++) {
        i++;
        tempRating = ((CoderRating) ratings.get(j)).getRating();
        tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
        if (tempRating != rating) {
          rating = tempRating;
          rank = i;
        }
        psIns.setLong(1, tempCoderId);
        psIns.setInt(2, roundId);
        psIns.setFloat(3, (float) 100 * ((float) (coderCount - rank) / coderCount));
        psIns.setInt(4, rank);
        count += psIns.executeUpdate();

        logger.info("... loaded " + count + " rating rank history ...");
      }
      logger.info("records loaded for rating rank history load: " + count);

    } catch (Exception e) {
      logger.error("load of 'coder_rank_history' table failed for rating rank for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeResultSet(rs);
      dbHelper.closeStatement(psSel);
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  /**
   * Loads the country_coder_rank table with information about
   * rating rank within a country.
   */
  public void loadCountryRatingRank(Connection dwConn, int roundId, int rankType, int ratingType, List list)
      throws Exception {
    logger.debug("loadCountryRatingRank called...");
    StringBuffer query = null;
    PreparedStatement psDel = null;
    PreparedStatement psIns = null;
    int count = 0;
    int coderCount = 0;
    List ratings = null;
    CoderRating curr = null;

    try {

      query = new StringBuffer(100);
      query.append(" DELETE");
      query.append(" FROM country_coder_rank");
      query.append(" WHERE coder_rank_type_id = " + rankType);
      query.append(" AND algo_rating_type_id = " + ratingType);
      psDel = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" INSERT");
      query.append(" INTO country_coder_rank (coder_id, percentile, rank, rank_no_tie, ");
      query.append("       country_code, coder_rank_type_id, algo_rating_type_id)");
      query.append(" VALUES (?, ?, ?, ?, ?, ?, ?)");
      psIns = dwConn.prepareStatement(query.toString());

      // delete all the records from the country ranking table
      psDel.executeUpdate();

      HashMap countries = new HashMap();
      String tempCode = null;
      List tempList = null;
      CoderRating temp = null;

      for (int i = 0; i < list.size(); i++) {
        temp = (CoderRating) list.get(i);
        if ((rankType == ACTIVE_RATING_RANK_TYPE_ID && temp.isActive()) ||
            rankType != ACTIVE_RATING_RANK_TYPE_ID) {
          tempCode = temp.getCountryCode();
          if (countries.containsKey(tempCode)) {
            tempList = (List) countries.get(tempCode);
          } else {
            tempList = new ArrayList(100);
          }
          tempList.add(list.get(i));
          countries.put(tempCode, tempList);
          tempList = null;
        }
      }

      for (Iterator it = countries.entrySet().iterator(); it.hasNext();) {
        ratings = (List) ((Map.Entry) it.next()).getValue();
        Collections.sort(ratings);
        coderCount = ratings.size();

        int i = 0;
        int rating = 0;
        int rank = 0;
        int size = ratings.size();
        int tempRating = 0;
        long tempCoderId = 0;
        for (int j = 0; j < size; j++) {
          i++;
          tempRating = ((CoderRating) ratings.get(j)).getRating();
          tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
          curr = (CoderRating) ratings.get(j);
          if (tempRating != rating) {
            rating = tempRating;
            rank = i;
          }
          psIns.setLong(1, tempCoderId);
          psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
          psIns.setInt(3, rank);
          psIns.setInt(4, j + 1);
          psIns.setString(5, curr.getCountryCode());
          psIns.setInt(6, rankType);
          psIns.setInt(7, ratingType);
          count += psIns.executeUpdate();

          logger.info("... loaded " + count + " country coder rating rank ...");
        }
      }
      logger.info("records loaded for country coder rating rank load: " + count);

    } catch (Exception e) {
      logger.error("load of 'country_coder_rank' table failed for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  /**
   * Loads the state_coder_rank table with information about
   * rating rank within a state.
   */
  public void loadStateRatingRank(Connection dwConn, int roundId, int rankType, int ratingType, List list)
      throws Exception {
    logger.debug("loadStateRatingRank called...");
    StringBuffer query = null;
    PreparedStatement psDel = null;
    // PreparedStatement psSel = null;
    PreparedStatement psIns = null;
    // ResultSet rs = null;
    int count = 0;
    int coderCount = 0;
    List ratings = null;
    CoderRating curr = null;

    try {

      query = new StringBuffer(100);
      query.append(" DELETE");
      query.append(" FROM state_coder_rank");
      query.append(" WHERE coder_rank_type_id = " + rankType);
      query.append(" AND algo_rating_type_id = " + ratingType);
      psDel = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" INSERT");
      query.append(
          " INTO state_coder_rank (coder_id, percentile, rank, rank_no_tie, state_code, coder_rank_type_id, algo_rating_type_id)");
      query.append(" VALUES (?, ?, ?, ?, ?, ?, ?)");
      psIns = dwConn.prepareStatement(query.toString());

      // delete all the records from the country ranking table
      psDel.executeUpdate();

      HashMap states = new HashMap();
      String tempCode = null;
      List tempList = null;
      CoderRating temp = null;

      for (int i = 0; i < list.size(); i++) {
        temp = (CoderRating) list.get(i);
        if ((rankType == ACTIVE_RATING_RANK_TYPE_ID && temp.isActive()) ||
            rankType != ACTIVE_RATING_RANK_TYPE_ID) {
          tempCode = temp.getStateCode();
          if (tempCode != null && !tempCode.trim().equals("")) {
            if (states.containsKey(tempCode)) {
              tempList = (List) states.get(tempCode);
            } else {
              tempList = new ArrayList(100);
            }
            tempList.add(list.get(i));
            states.put(tempCode, tempList);
            tempList = null;
          }
        }
      }

      for (Iterator it = states.entrySet().iterator(); it.hasNext();) {
        ratings = (List) ((Map.Entry) it.next()).getValue();
        Collections.sort(ratings);
        coderCount = ratings.size();

        int i = 0;
        int rating = 0;
        int rank = 0;
        int size = ratings.size();
        int tempRating = 0;
        long tempCoderId = 0;
        for (int j = 0; j < size; j++) {
          i++;
          tempRating = ((CoderRating) ratings.get(j)).getRating();
          tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
          curr = ((CoderRating) ratings.get(j));
          if (tempRating != rating) {
            rating = tempRating;
            rank = i;
          }
          psIns.setLong(1, tempCoderId);
          psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
          psIns.setInt(3, rank);
          psIns.setInt(4, j + 1);
          psIns.setString(5, curr.getStateCode());
          psIns.setInt(6, rankType);
          psIns.setInt(7, ratingType);
          count += psIns.executeUpdate();

          logger.info("... loaded " + count + " state coder rating rank ...");
        }
      }
      logger.info("records loaded for state coder rating rank load: " + count);

    } catch (Exception e) {
      logger.error("load of 'state_coder_rank' table failed for rating rank for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }

  /**
   * Loads the school_coder_rank table with information about
   * rating rank within a school.
   */
  public void loadSchoolRatingRank(Connection dwConn, int roundId, int rankType, int ratingType, List list) throws Exception {
    logger.debug("loadSchoolRatingRank called...");
    StringBuffer query = null;
    PreparedStatement psDel = null;
    PreparedStatement psIns = null;
    int count = 0;
    int coderCount = 0;
    List ratings = null;
    CoderRating curr = null;

    try {

      query = new StringBuffer(100);
      query.append(" DELETE");
      query.append(" FROM school_coder_rank");
      query.append(" WHERE coder_rank_type_id = " + rankType);
      query.append(" AND algo_rating_type_id = " + ratingType);
      psDel = dwConn.prepareStatement(query.toString());

      query = new StringBuffer(100);
      query.append(" INSERT");
      query.append(
          " INTO school_coder_rank (coder_id, percentile, rank, rank_no_tie, school_id, coder_rank_type_id, algo_rating_type_id)");
      query.append(" VALUES (?, ?, ?, ?, ?, ?, ?)");
      psIns = dwConn.prepareStatement(query.toString());

      // delete all the records from the country ranking table
      psDel.executeUpdate();

      HashMap schools = new HashMap();
      Long tempId = null;
      List tempList = null;
      CoderRating temp = null;

      for (int i = 0; i < list.size(); i++) {
        temp = (CoderRating) list.get(i);
        if ((rankType == ACTIVE_RATING_RANK_TYPE_ID && temp.isActive()) ||
            rankType != ACTIVE_RATING_RANK_TYPE_ID) {
          if (temp.getSchoolId() > 0) {
            tempId = new Long(temp.getSchoolId());
            if (schools.containsKey(tempId)) {
              tempList = (List) schools.get(tempId);
            } else {
              tempList = new ArrayList(10);
            }
            tempList.add(list.get(i));
            schools.put(tempId, tempList);
            tempList = null;
          }
        }
      }

      for (Iterator it = schools.entrySet().iterator(); it.hasNext();) {
        ratings = (List) ((Map.Entry) it.next()).getValue();
        Collections.sort(ratings);
        coderCount = ratings.size();

        int i = 0;
        int rating = 0;
        int rank = 0;
        int size = ratings.size();
        int tempRating = 0;
        long tempCoderId = 0;
        for (int j = 0; j < size; j++) {
          i++;
          tempRating = ((CoderRating) ratings.get(j)).getRating();
          tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
          curr = (CoderRating) ratings.get(j);
          if (tempRating != rating) {
            rating = tempRating;
            rank = i;
          }
          psIns.setLong(1, tempCoderId);
          psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
          psIns.setInt(3, rank);
          psIns.setInt(4, j + 1);
          psIns.setLong(5, curr.getSchoolId());
          psIns.setInt(6, rankType);
          psIns.setInt(7, ratingType);
          count += psIns.executeUpdate();

          logger.info("... loaded " + count + " school coder rating rank ...");
        }
      }
      logger.info("records loaded for school coder rating rank load: " + count);

    } catch (Exception e) {
      logger.error("load of 'school_coder_rank' table failed for rating rank for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeStatement(psIns);
      dbHelper.closeStatement(psDel);
    }
  }
}
