package com.topcoder.ratings.events;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;

@Repository
public class EventConfig {
  @Value("${ratings.auth0ProxyServerUrl}")
  private  String auth0ProxyServerUrl;

  @Value("${ratings.clientId}")
  private  String clientId;

  @Value("${ratings.clientSecret}")
  private  String clientSecret;

  @Value("${ratings.audience}")
  private  String audience;

  @Value("${ratings.auth0Url}")
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
