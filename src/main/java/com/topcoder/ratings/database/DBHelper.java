package com.topcoder.ratings.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;

/**
 * A class for Database connection
 *
 */
public class DBHelper extends JdbcTemplate {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public DBHelper() {
  }

  public Connection getConnection(String dbFlag) throws CannotGetJdbcConnectionException {
    try {
      logger.debug("getting DB connection");
      DataSource dataSource = dbFlag == "OLTP" ? dataSourceOLTP() : dataSourceDW();
      Connection con = dataSource.getConnection();
      if (con == null) {
        throw new IllegalStateException("DataSource returned null from getConnection(): " + getDataSource());
      }
      con.setAutoCommit(false);
      return con;
    } catch (SQLException ex) {
      logger.error("error while getting the JDBC connection", ex);
      throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
    } catch (IllegalStateException ex) {
      logger.error("error while getting the JDBC connection", ex);
      throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
    }
  }

  public void closeConnection(@Nullable Connection con) throws SQLException {
    if (con != null) {
      try {
        con.close();
      } catch (SQLException ex) {
        logger.error("error while getting the JDBC connection", ex);
        throw new SQLException("Failed to close the JDBC connection", ex);
      } catch (Throwable ex) {
        logger.error("error while getting the JDBC connection", ex);
        throw new SQLException("Unexpected exception on closing JDBC Connection", ex);
      }
    }
  }

  public void closeStatement(@Nullable Statement stmt) throws SQLException {
    if (stmt != null) {
      try {
        stmt.close();
        stmt = null;
      } catch (SQLException ex) {
        throw new SQLException("Failed to close the JDBC Statement", ex);
      } catch (Throwable ex) {
        throw new SQLException("Unexpected exception on closing JDBC Statement", ex);
      }
    }
  }

  public void closeResultSet(@Nullable ResultSet rs) throws SQLException {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException ex) {
        throw new SQLException("Failed to close the JDBC ResultSet", ex);
      } catch (Throwable ex) {
        throw new SQLException("Unexpected exception on closing JDBC ResultSet", ex);
      }
    }
  }

  @ConfigurationProperties(prefix = "spring.datasource")
  public DataSource dataSourceOLTP() {
    return DataSourceBuilder.create().build();
  }

  @ConfigurationProperties(prefix = "spring.datasource-dw")
  public DataSource dataSourceDW() {
    return DataSourceBuilder.create().build();
  }
}
