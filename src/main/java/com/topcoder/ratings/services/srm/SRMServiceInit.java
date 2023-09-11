package com.topcoder.ratings.services.srm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;

import com.topcoder.ratings.database.DBHelper;
import com.topcoder.ratings.events.EventHelper;

@Service
@EnableAsync
public class SRMServiceInit {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  Connection conn;
  Connection oltpConn;

  @Autowired
  EventHelper eventHelper;

  public final int DIVISION_ADMIN = -1;
  public final int DIVISION_ONE = 1;
  public final int DIVISION_TWO = 2;

  @Async
  @PostMapping(path = "/mm/calculcate", produces = "application/json")
  public void calculateSRMRatings(int roundId, DBHelper dbHelper) throws Exception {
    try {
      logger.info("=== start rating calculate for round " + roundId + " ===");

      oltpConn = dbHelper.getConnection("OLTP");
      oltpConn.setAutoCommit(false);
      
      // int roundId, int division, boolean isFinal, boolean byDiv, int ratingType
      logger.debug("=== calculation for divison by " + DIVISION_ONE + " ===");
      doIt(oltpConn, roundId, DIVISION_ONE, true, true, 1);

      logger.debug("=== calculation for divison by " + DIVISION_TWO + " ===");
      doIt(oltpConn, roundId, DIVISION_TWO, true, true, 1);
      
      logger.debug("=== going for commit ===");
      oltpConn.commit();
      
      logger.info("=== end rating calculate for round " + roundId + " ===");

      logger.info("=== sending message for round: " + roundId + " ===");
      eventHelper.fireEvent(roundId, "RATINGS_CALCULATION", "SUCCESS");

      logger.info("=== complete rating calculate for round " + roundId + " ===");
      
    } catch (SQLException e) {
      oltpConn.rollback();
      logger.error("Failed to run the Marathon Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
    } finally {
      dbHelper.closeConnection(conn);
    }
  }

  private void doIt(Connection conn, int roundId, int divisionId, boolean isFinal, boolean byDiv, int ratingType) throws Exception {
        
    PreparedStatement ps = null;
    ResultSet rs2 = null;
    
    StringBuffer sqlStr = new StringBuffer(400);
    
    int room, newrating, newvol, coder, retVal, newratingnovol, oldrating, oldvol;
    
    Vector names, ratings, volatilities, timesplayed, scores, endratings, endvols, endratingsnovol, ratingsplusprov, namesplusprov, volatilitiesplusprov, timesplayedplusprov, scoresplusprov, stringnames, stringnamesplusprov;
    
    ArrayList results = new ArrayList();
    ArrayList resultsplusprov = new ArrayList();

    try {
      Timestamp currentTime = getCurrentTimestamp(conn);

      names = new Vector();
      namesplusprov = new Vector();
      ratings = new Vector();
      ratingsplusprov = new Vector();
      volatilities = new Vector();
      volatilitiesplusprov = new Vector();
      timesplayed = new Vector();
      timesplayedplusprov = new Vector();
      scores = new Vector();
      scoresplusprov = new Vector();
      stringnames = new Vector();
      stringnamesplusprov = new Vector();

      //get a bunch of info of coders in the room
      if (byDiv) {
        sqlStr.replace(0, sqlStr.length(), "SELECT ra.coder_id,ra.rating,ra.vol,ra.num_ratings,rr.point_total,rr.attended,u.handle ");
        sqlStr.append("FROM algo_rating ra, user u,room_result rr, room r ");
        sqlStr.append("WHERE rr.round_id = ? and u.user_id=ra.coder_id and rr.coder_id=ra.coder_id and r.room_id = rr.room_id and r.division_id = ? and ra.algo_rating_type_id = ?");

        ps = conn.prepareStatement(sqlStr.toString());
        ps.setInt(1, roundId);
        ps.setInt(2, divisionId);
        ps.setInt(3, ratingType);
        rs2 = ps.executeQuery();
      } else {
        sqlStr.replace(0, sqlStr.length(), "SELECT ra.coder_id,ra.rating,ra.vol,ra.num_ratings,rr.point_total,rr.attended,u.handle ");
        sqlStr.append("FROM algo_rating ra, user u,room_result rr, room r ");
        sqlStr.append("WHERE rr.round_id = ? and u.user_id=ra.coder_id and rr.coder_id=ra.coder_id and r.room_id = rr.room_id and r.division_id <> " + DIVISION_ADMIN + " and ra.algo_rating_type_id = ? ");

        ps = conn.prepareStatement(sqlStr.toString());
        ps.setInt(1, roundId);
        ps.setInt(2, ratingType);
        rs2 = ps.executeQuery();
      }

      //add each coder and his info to the vectors.
      String handle = "";

      while (rs2.next()) {
        if (rs2.getString(6).equals("Y")) {
          namesplusprov.add("" + rs2.getInt(1));
          timesplayedplusprov.add(new Integer(rs2.getInt(4)));
          scoresplusprov.add(new Double(rs2.getDouble(5)));
          
          handle = rs2.getString(7);
          
          if (handle == null) handle = "";
          
          stringnamesplusprov.add(handle);

          if (rs2.getInt(4) > 0) {
            ratingsplusprov.add(new Double(rs2.getInt(2)));
            names.add("" + rs2.getInt(1));
            ratings.add(new Double(rs2.getInt(2)));
            
            int vol = rs2.getInt(3);
            vol = vol == 0 ? initialVolatility : vol;
            
            volatilities.add(new Double(vol));
            volatilitiesplusprov.add(new Double(vol));
            timesplayed.add(new Integer(rs2.getInt(4)));
            scores.add(new Double(rs2.getDouble(5)));
            stringnames.add(handle);
          } else {
            volatilitiesplusprov.add(new Double(initialVolatility));
            ratingsplusprov.add(new Double(1200.0));
          }
        }
      }

      //run qubits rating algorithm on them
      logger.info("Ratings for non-provisional: round " + roundId + ", div " + divisionId + ":");

      results = rateEvent(names, ratings, volatilities, timesplayed, scores, stringnames, false);

      logger.info("Ratings for provisional: round " + roundId + ", div " + divisionId + ":");

      resultsplusprov = rateEvent(namesplusprov, ratingsplusprov, volatilitiesplusprov, timesplayedplusprov, scoresplusprov, stringnamesplusprov, true);

      //if isFinal, update the database
      if (!isFinal) {
          return;
      }

      endratings = (Vector) results.get(2);
      endvols = (Vector) results.get(1);
      endratingsnovol = (Vector) results.get(0);

      while (endratings.size() > 0) {
        newrating = (int)Math.round(((Double) endratings.remove(0)).doubleValue());
        newratingnovol = (int)Math.round(((Double) endratingsnovol.remove(0)).doubleValue());
        newvol = (int)Math.round(((Double) endvols.remove(0)).doubleValue());
        oldrating = (int)Math.round(((Double) ratings.remove(0)).doubleValue());
        oldvol = (int)Math.round(((Double) volatilities.remove(0)).doubleValue());
        coder = (new Integer(names.remove(0).toString())).intValue();

        logger.debug("updating coder " + coder + " to " + newrating);

        sqlStr.replace(0, sqlStr.length(), "UPDATE algo_rating SET rating = ?, vol = ? , num_ratings = num_ratings + 1, ");
        sqlStr.append(" round_id = ? WHERE coder_id = ? and algo_rating_type_id = ?");
        
        ps = conn.prepareStatement(sqlStr.toString());
        ps.setDouble(1, newrating);
        ps.setInt(2, newvol);
        ps.setInt(3, roundId);
        ps.setInt(4, coder);
        ps.setInt(5, ratingType);
        
        retVal = ps.executeUpdate();
        
        ps.close();
        ps = null;

        if (retVal != 1)
          logger.error("ERROR: New rating not updated in CODER_RATING table");

        sqlStr.replace(0, sqlStr.length(), "UPDATE room_result SET old_rating = ?, old_vol = ?, new_rating = ?, new_vol = ?, rated_flag=1 WHERE round_id = ? AND coder_id = ?");
        
        ps = conn.prepareStatement(sqlStr.toString());
        ps.setDouble(1, oldrating);
        ps.setInt(2, oldvol);
        ps.setDouble(3, newrating);
        ps.setInt(4, newvol);
        ps.setInt(5, roundId);
        ps.setInt(6, coder);
        
        retVal = ps.executeUpdate();
        ps.close();
        ps = null;

        if (retVal != 1)
          logger.error("ERROR: New rating not updated in ROOM_STATUS (or ROOM_RESULT) table");
      } // end while loop over endratings


      endratings = (Vector) resultsplusprov.get(2);
      endvols = (Vector) resultsplusprov.get(1);
      endratingsnovol = (Vector) resultsplusprov.get(0);
      names = (Vector) resultsplusprov.get(3);
      ratings = (Vector) resultsplusprov.get(4);
      volatilities = (Vector) resultsplusprov.get(5);

      while (endratings.size() > 0) {
        newrating = (int)Math.round(((Double) endratings.remove(0)).doubleValue());
        newratingnovol = (int)Math.round(((Double) endratingsnovol.remove(0)).doubleValue());
        newvol = (int)Math.round(((Double) endvols.remove(0)).doubleValue());
        oldrating = (int)Math.round(((Double) ratings.remove(0)).doubleValue());
        oldvol = (int)Math.round(((Double) volatilities.remove(0)).doubleValue());
        coder = (new Integer(names.remove(0).toString())).intValue();
        logger.debug("updating coder " + coder + " to " + newrating);

        sqlStr.replace(0, sqlStr.length(), "UPDATE algo_rating SET rating = ?, vol = ? , num_ratings = num_ratings + 1, ");
        sqlStr.append(" round_id = ? WHERE coder_id = ? and algo_rating_type_id = ?");
        
        ps = conn.prepareStatement(sqlStr.toString());
        ps.setDouble(1, newrating);
        ps.setInt(2, newvol);
        ps.setInt(3, roundId);
        ps.setInt(4, coder);
        ps.setInt(5, ratingType);
        
        retVal = ps.executeUpdate();
        
        ps.close();
        ps = null;

        if (retVal != 1)
          logger.error("ERROR: New rating not updated in RATING table");

        sqlStr.replace(0, sqlStr.length(), "UPDATE room_result SET old_rating = ?, old_vol = ?, new_rating = ?, new_vol = ?, rated_flag=1 WHERE round_id = ? AND coder_id = ?");
        
        ps = conn.prepareStatement(sqlStr.toString());
        ps.setDouble(1, oldrating);
        ps.setInt(2, oldvol);
        ps.setDouble(3, newrating);
        ps.setInt(4, newvol);
        ps.setInt(5, roundId);
        ps.setInt(6, coder);
        
        retVal = ps.executeUpdate();
        
        ps.close();
        ps = null;

        if (retVal != 1)
          logger.error("ERROR: New rating not updated in ROOM_STATUS table");
      }  // end while loop over endratings

    } catch (Exception e) {
      logger.error("Failed to run the SRM Ratings for round " + roundId);
      logger.error(e.getMessage());
      logger.error("", e);
      throw e;
    } finally {
      rs2.close();
    }
  }

  //-------------FUNCTIONS AND VARIABLES USED BY QUBITS RATING SYSTEM-----------------------
  int STEPS = 100;
  
  double initialScore = 1200.0;
  double oneStdDevEquals = 1200.0; /* rating points */
  
  int initialVolatility = 515;
  double firstVolatility = 385;
  double initialWeight = 0.60;
  double finalWeight = 0.18;
  double volatilityWeight = 0;
  double matchStdDevEquals = 500.0;	/* rating points */
  
  int people = 0;
  double sqiv = 0.0;
  int people2 = 0;
  double sqfv = 0.0;
  double fv = 0.0;
  int people3 = 0;
  double sb = 0.0;
  int people4 = 0;
  double sqdf = 0.0;

  /**
   *
   * @param names
   * @param ratings
   * @param volatilities
   * @param timesPlayed
   * @param scores
   * @param stringnames
   * @param prov if true, the list includes people that have been never been rated before this round
   * @return
   */
  private ArrayList rateEvent(Vector names, Vector ratings, Vector volatilities, Vector timesPlayed, Vector scores, Vector stringnames, boolean prov) {
    Vector eranks = new Vector();
    Vector eperf = new Vector();
    Vector ranks = new Vector();
    Vector perf = new Vector();
    Vector newVolatility = new Vector();
    Vector newRating = new Vector();
    Vector newRatingWithVol = new Vector();
    int i, j;
    double aveVol = 0,rating,vol;

    /* COMPUTE AVERAGE RATING */
    double rave = 0.0;
    for (i = 0; i < ratings.size(); i++) {
        rave += ((Double) ratings.elementAt(i)).doubleValue();
    }
    rave /= ratings.size();

    /* COMPUTE COMPETITION FACTOR */
    double rtemp = 0, vtemp = 0;
    for (i = 0; i < ratings.size(); i++) {
      vtemp += sqr(((Double) volatilities.elementAt(i)).doubleValue());
      rtemp += sqr(((Double) ratings.elementAt(i)).doubleValue() - rave);
    }
    if (ratings.size() > 1)
      matchStdDevEquals = Math.sqrt(vtemp / ratings.size() + rtemp / (ratings.size() - 1));
    else
      matchStdDevEquals = 0;

    /* COMPUTE EXPECTED RANKS */
    for (i = 0; i < names.size(); i++) {
      ranks.addElement(new Double(0));
      perf.addElement(new Double(0));
      double est = 0.5;
      double myskill = (((Double) ratings.elementAt(i)).doubleValue() - initialScore) / oneStdDevEquals;
      double mystddev = ((Double) volatilities.elementAt(i)).doubleValue() / oneStdDevEquals;
      for (j = 0; j < names.size(); j++) {
        est += winprobabilitynew(((Double) ratings.elementAt(j)).doubleValue(), ((Double) ratings.elementAt(i)).doubleValue() , ((Double) volatilities.elementAt(j)).doubleValue(), ((Double) volatilities.elementAt(i)).doubleValue() );
      }
      eranks.addElement(new Double(est));
      eperf.addElement(new Double(-normsinv((est - .5) / names.size())));
    }

    /* COMPUTE ACTUAL RANKS */
    for (i = 0; i < names.size();) {
      double max = Double.NEGATIVE_INFINITY;
      int count = 0;

      for (j = 0; j < names.size(); j++) {
        if (((Double) scores.elementAt(j)).doubleValue() >= max && ((Double) ranks.elementAt(j)).doubleValue() == 0) {
          if (((Double) scores.elementAt(j)).doubleValue() == max)
            count++;
          else
            count = 1;
          max = ((Double) scores.elementAt(j)).doubleValue();
        }
      }
      for (j = 0; j < names.size(); j++) {
        if (((Double) scores.elementAt(j)).doubleValue() == max) {
          ranks.setElementAt(new Double(i + 0.5 + count / 2.0), j);
          perf.setElementAt(new Double(-normsinv((i + count / 2.0) / names.size())), j);
        }
      }
      i += count;
    }

    /* UPDATE RATINGS */
    for (i = 0; i < names.size(); i++) {
      double diff = ((Double) perf.elementAt(i)).doubleValue() - ((Double) eperf.elementAt(i)).doubleValue();
      sqdf += diff * diff;
      people4++;
      double oldrating = ((Double) ratings.elementAt(i)).doubleValue();
      double performedAs = oldrating + diff * matchStdDevEquals;
      double weight = (initialWeight - finalWeight) / (((Integer) timesPlayed.elementAt(i)).intValue() + 1) + finalWeight;

      //get weight - reduce weight for highly rated people
      weight = 1 / (1 - weight) - 1;
      if (oldrating >= 2000 && oldrating < 2500) weight = weight * 4.5 / 5.0;
      if (oldrating >= 2500) weight = weight * 4.0 / 5.0;

      double newrating = (oldrating + weight * performedAs) / (1 + weight);

      //get and inforce a cap
      double cap = 150 + 1500 / (2 + ((Integer) timesPlayed.elementAt(i)).intValue());
      if (oldrating - newrating > cap) newrating = oldrating - cap;
      if (newrating - oldrating > cap) newrating = oldrating + cap;
      if (newrating < 1) newrating = 1;

      newRating.addElement(new Double(newrating));

      if (((Integer) timesPlayed.elementAt(i)).intValue() != 0) {
        if (((Integer) timesPlayed.elementAt(i)).intValue() == 1) {
          fv += (performedAs - oldrating);
          sqfv += (performedAs - oldrating) * (performedAs - oldrating);
          people2++;
        } else {
          sb += (performedAs - oldrating) * (performedAs - oldrating);
          people3++;
        }
        double oldVolatility = ((Double) volatilities.elementAt(i)).doubleValue();
        newVolatility.addElement(new Double(Math.sqrt((oldVolatility*oldVolatility) / (1+weight) + ((newrating-oldrating)*(newrating-oldrating))/ weight)));
      } else {
        sqiv += (performedAs - oldrating) * (performedAs - oldrating);
        people++;
        newVolatility.addElement(new Double(firstVolatility));
      }
    }
    aveVol = 385;

    for (i = 0; i < newVolatility.size(); i++) {
      rating = ((Double) newRating.elementAt(i)).doubleValue();
      vol = ((Double) newVolatility.elementAt(i)).doubleValue();
      newRatingWithVol.addElement(new Double(rating + volatilityWeight * (aveVol - vol)));
      //if this includes includes the people that have never been rated before
      //and this particular person has been rated before them, then pull them out of the lists
      if (prov && ((Integer) timesPlayed.elementAt(i)).intValue() > 0) {
        names.remove(i);
        timesPlayed.remove(i);
        ratings.remove(i);
        volatilities.remove(i);
        eranks.remove(i);
        eperf.remove(i);
        scores.remove(i);
        ranks.remove(i);
        perf.remove(i);
        newRating.remove(i);
        newVolatility.remove(i);
        newRatingWithVol.remove(i);
        stringnames.remove(i);
        i--;
      }
    }

    logger.info("Handle   Player  # Rate Vol Es.R E.SD  Score  Ac.R A.SD D.SD N.RT N.V N.VR");
    for (i = 0; i < names.size(); i++) {
      if (!prov || (prov && ((Integer) timesPlayed.elementAt(i)).intValue() == 0))
        logger.info(
          fS1((String) stringnames.elementAt(i)) + " " +
          ((String) names.elementAt(i)) + "  " +
          ((Integer) timesPlayed.elementAt(i)).intValue() + " " +
          in4((Double) ratings.elementAt(i)) + " " +
          ((Double) volatilities.elementAt(i)).intValue() + " " +
          rat((Double) eranks.elementAt(i)) + " " +
          fD2((Double) eperf.elementAt(i)) + " " +
          scr((Double) scores.elementAt(i)) + " " +
          rat((Double) ranks.elementAt(i)) + " " +
          fD2((Double) perf.elementAt(i)) + " " +
          fD2(new Double(((Double) perf.elementAt(i)).doubleValue() -
          ((Double) eperf.elementAt(i)).doubleValue())) +
          " " + in4((Double) newRating.elementAt(i)) + " " +
          ((Double) newVolatility.elementAt(i)).intValue()
          + " " + in4((Double) newRatingWithVol.elementAt(i))
        );
    }

    ArrayList al = new ArrayList();
    al.add(newRating);
    al.add(newVolatility);
    al.add(newRatingWithVol);
    al.add(names);
    al.add(ratings);
    al.add(volatilities);
    return al;
  }

  private String fS1(String in) {
    if (in.length() > 8) return in.substring(0, 8);
    while (in.length() < 8) in = in + " ";
    return in;
  }

  private String fD1(Double in) {
    DecimalFormat oneDigit = new DecimalFormat("0.0");
    return oneDigit.format(in.doubleValue());
  }

  private String fD2(Double in) {
    DecimalFormat oneDigit = new DecimalFormat("0.0");
    DecimalFormat twoDigits = new DecimalFormat("0.00");
    if (in.doubleValue() < 0)
      return oneDigit.format(in.doubleValue());
    return twoDigits.format(in.doubleValue());
  }

  private String scr(Double in) {
    DecimalFormat sixDigits = new DecimalFormat("0000.00");
    DecimalFormat fiveDigits = new DecimalFormat("000.00");
    if (in.doubleValue() < 0)
      return fiveDigits.format(in.doubleValue());
    return sixDigits.format(in.doubleValue());
  }

  private String rat(Double in) {
    DecimalFormat twoDigits = new DecimalFormat("00.0");
    DecimalFormat threeDigits = new DecimalFormat("000.");
    if (in.doubleValue() < 100)
      return twoDigits.format(in.doubleValue());
    return threeDigits.format(in.doubleValue());
  }

  private String in4(Double in) {
    DecimalFormat oneDigit = new DecimalFormat("0000");
    return oneDigit.format(in.doubleValue());
  }

  private double sqr(double j) {
    return j * j;
  }
  private double normsinv(double p) {
    return normsinvnew(p);
  }
  
  private double normsinvnew(double p) {
  /* ********************************************
    * Original algorythm and Perl implementation can
    * be found at:
    * http://www.math.uio.no/~jacklam/notes/invnorm/index.html
    * Author:
    *  Peter J. Acklam
    *  jacklam@math.uio.no
    * ****************************************** */
      
    // Define break-points.
    // variable for result
    if(p <= 0) return Double.NEGATIVE_INFINITY;
    else if(p >= 1) return Double.POSITIVE_INFINITY;
    
    double z = 0;

    // Rational approximation for lower region:
    if( p < P_LOW )
    {
      double q  = Math.sqrt(-2*Math.log(p));
      z = (((((NORMINV_C[0]*q+NORMINV_C[1])*q+NORMINV_C[2])*q+NORMINV_C[3])*q+NORMINV_C[4])*q+NORMINV_C[5]) / ((((NORMINV_D[0]*q+NORMINV_D[1])*q+NORMINV_D[2])*q+NORMINV_D[3])*q+1);
    }
    // Rational approximation for upper region:
    else if ( P_HIGH < p )
    {
      double q  = Math.sqrt(-2*Math.log(1-p));
      z = -(((((NORMINV_C[0]*q+NORMINV_C[1])*q+NORMINV_C[2])*q+NORMINV_C[3])*q+NORMINV_C[4])*q+NORMINV_C[5]) / ((((NORMINV_D[0]*q+NORMINV_D[1])*q+NORMINV_D[2])*q+NORMINV_D[3])*q+1);
    }
    // Rational approximation for central region:
    else
    {
      double q = p - 0.5D;
      double r = q * q;
      z = (((((NORMINV_A[0]*r+NORMINV_A[1])*r+NORMINV_A[2])*r+NORMINV_A[3])*r+NORMINV_A[4])*r+NORMINV_A[5])*q / (((((NORMINV_B[0]*r+NORMINV_B[1])*r+NORMINV_B[2])*r+NORMINV_B[3])*r+NORMINV_B[4])*r+1);
    }
    
    z = refine(z, p);
    return z;
  }
  

  private static final double P_LOW  = 0.02425D;
  private static final double P_HIGH = 1.0D - P_LOW;

  // Coefficients in rational approximations.
  private static final double NORMINV_A[] =
  { -3.969683028665376e+01,  2.209460984245205e+02,
  -2.759285104469687e+02,  1.383577518672690e+02,
  -3.066479806614716e+01,  2.506628277459239e+00 };

  private static final double NORMINV_B[] =
  { -5.447609879822406e+01,  1.615858368580409e+02,
  -1.556989798598866e+02,  6.680131188771972e+01,
  -1.328068155288572e+01 };

  private static final double NORMINV_C[] =
  { -7.784894002430293e-03, -3.223964580411365e-01,
  -2.400758277161838e+00, -2.549732539343734e+00,
  4.374664141464968e+00,  2.938163982698783e+00 };

  private static final double NORMINV_D[] =
  { 7.784695709041462e-03,  3.224671290700398e-01,
  2.445134137142996e+00,  3.754408661907416e+00 };

  public static double erf(double z) {
    double t = 1.0 / (1.0 + 0.5 * Math.abs(z));

      // use Horner's method
    double ans = 1 - t * Math.exp( -z*z   -   1.26551223 +
                                    t * ( 1.00002368 +
                                    t * ( 0.37409196 + 
                                    t * ( 0.09678418 + 
                                    t * (-0.18628806 + 
                                    t * ( 0.27886807 + 
                                    t * (-1.13520398 + 
                                    t * ( 1.48851587 + 
                                    t * (-0.82215223 + 
                                    t * ( 0.17087277))))))))));
    if (z >= 0) return  ans;
    else        return -ans;
  }
  
  public static double erfc(double z) {
    return 1.0 - erf(z);
  }

  public static double refine(double x, double d)
  {
    if( d > 0 && d < 1)
    {
      double e = 0.5D * erfc(-x/Math.sqrt(2.0D)) - d;
      double u = e * Math.sqrt(2.0D*Math.PI) * Math.exp((x*x)/2.0D);
      x = x - u/(1.0D + x*u/2.0D);
    }
    return x;
  }

  private double norminv(double p, double mean, double stddev) {
    return mean + normsinv(p) * stddev;
  }
  
  private double winprobabilitynew(double r1, double r2, double v1, double v2) {
    return (erf((r1-r2)/Math.sqrt(2.0*(v1*v1+v2*v2)))+1.0)*.5;
  }

  public final Timestamp getCurrentTimestamp(Connection conn) throws Exception {
    Timestamp result = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = conn.prepareStatement("SELECT CURRENT FROM dual");
      rs = ps.executeQuery();

      if (rs.next()) result = rs.getTimestamp(1);
    } catch (Exception e) {
      throw new Exception("common.Common:getCurrentDate: Error " + e);
    } finally {
      ps.close();
      rs.close();
    }
    return result;
  }
}
