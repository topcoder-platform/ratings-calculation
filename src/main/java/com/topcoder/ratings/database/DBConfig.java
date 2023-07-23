package com.topcoder.ratings.database;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class DBConfig {
    @Bean(name = "oltp")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSourceOLTP() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "dw")
    @ConfigurationProperties(prefix = "spring.datasource-dw")
    public DataSource dataSourceDW() {
        return DataSourceBuilder.create().build();
    }
}