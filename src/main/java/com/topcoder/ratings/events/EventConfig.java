package com.topcoder.ratings.events;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;

@Repository
public class EventConfig {
  @Value("${spring.security.oauth2.resourceserver.jwt.auth0ProxyServerUrl}")
  private  String auth0ProxyServerUrl;

  @Value("${spring.security.oauth2.resourceserver.jwt.clientId}")
  private  String clientId;

  @Value("${spring.security.oauth2.resourceserver.jwt.clientSecret}")
  private  String clientSecret;

  @Value("${spring.security.oauth2.resourceserver.jwt.audience}")
  private  String audience;

  @Value("${spring.security.oauth2.resourceserver.jwt.auth0Url}")
  private  String auth0Url;

  @Value("${ratings.busApi}")
  private  String busApi;

  @Value("${ratings.eventTopic}")
  private  String eventTopic;

  @Bean(name = "event")
  public Map<String, String> getEventConfigs() {
    Map<String, String> eventConfig = new HashMap<>();
    eventConfig.put("auth0ProxyServerUrl", auth0ProxyServerUrl);
    eventConfig.put("clientId", clientId);
    eventConfig.put("clientSecret", clientSecret);
    eventConfig.put("audience", audience);
    eventConfig.put("auth0Url", auth0Url);
    eventConfig.put("busApi", busApi);
    eventConfig.put("eventTopic", eventTopic);

    return eventConfig;
  }
}
