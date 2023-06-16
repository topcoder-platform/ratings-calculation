package com.topcoder.ratings.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;


/**
 * A class for Database connection
 *
 */
public class DBHelper extends JdbcTemplate{
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public DBHelper() {}
  
  public Connection getConnection() throws CannotGetJdbcConnectionException {
        try {
            Connection con = getDataSource().getConnection();
            if (con == null) {
                throw new IllegalStateException("DataSource returned null from getConnection(): " + getDataSource());
            }
            con.setAutoCommit(false);
            return con;
        } catch (SQLException ex) {
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        } catch (IllegalStateException ex) {
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        }
    }

    public void closeConnection(@Nullable Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException ex) {
                logger.error("Could not close JDBC Connection", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC Connection", ex);
            }
        }
    }

    public void closeStatement(@Nullable Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
                stmt = null;
            } catch (SQLException ex) {
                logger.error("Could not close JDBC Statement", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC Statement", ex);
            }
        }
    }

    public void closeResultSet(@Nullable ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                logger.error("Could not close JDBC ResultSet", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC ResultSet", ex);
            }
        }
    }   
}
