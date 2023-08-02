package com.topcoder.ratings.database;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;

@Repository
public class DBConfig {
    @Value("${oltp.datasource.jdbcUrl}")
    private String oltp_jdbc_url;

    @Value("${oltp.datasource.username}")
    private String oltp_username;

    @Value("${oltp.datasource.password}")
    private String oltp_password;

    @Value("${dw.datasource.jdbcUrl}")
    private String dw_jdbc_url;

    @Value("${dw.datasource.username}")
    private String dw_username;

    @Value("${dw.datasource.password}")
    private String dw_password;

    @Bean(name = "oltp")
    public DataSource dataSourceOLTP() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.url(oltp_jdbc_url);
        dataSourceBuilder.username(oltp_username);
        dataSourceBuilder.password(oltp_password);
        return dataSourceBuilder.build();
    }

    @Bean(name = "dw")
    @ConfigurationProperties(prefix = "spring.datasource-dw")
    public DataSource dataSourceDW() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.url(dw_jdbc_url);
        dataSourceBuilder.username(dw_username);
        dataSourceBuilder.password(dw_password);
        return dataSourceBuilder.build();
    }
}