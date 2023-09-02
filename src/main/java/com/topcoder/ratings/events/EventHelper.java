package com.topcoder.ratings.events;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class EventHelper {
  private final Logger logger = LoggerFactory.getLogger(EventHelper.class);

  private String token;

  @Autowired
  private EventConfig eventConfig;

  public String generateAuthToken() throws Exception {
    if (token == null) {
      logger.info("=== fetching new token ===");
      
      Map<String, String> eventConfigs = eventConfig.getEventConfigs();

      CloseableHttpClient httpClient = HttpClients.createDefault();

      HttpPost postRequest = new HttpPost(eventConfigs.get("auth0ProxyServerUrl"));

      postRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");

      // Generate the request body to get the token
      Map<String, String> reqBodyMap = new HashMap<String, String>();
      reqBodyMap.put("client_id", eventConfigs.get("clientId"));
      reqBodyMap.put("client_secret", eventConfigs.get("clientSecret"));
      reqBodyMap.put("grant_type", "client_credentials");
      reqBodyMap.put("audience",  eventConfigs.get("audience"));
      reqBodyMap.put("auth0_url",  eventConfigs.get("auth0Url"));

      // convert the request body to jsonString
      ObjectMapper objectMapper = new ObjectMapper();
      String reqBodyJson = objectMapper.writeValueAsString(reqBodyMap);

      postRequest.setEntity(new StringEntity(reqBodyJson));
      CloseableHttpResponse response = httpClient.execute(postRequest);

      String result = EntityUtils.toString(response.getEntity());

      // generate JSON object out of response string
      JSONObject myObject = new JSONObject(result);

      token = myObject.getString("access_token");
    } else {
      logger.info("=== using existing token ===");
      DecodedJWT decodedJWT = JWT.decode(token);
      Date tokenExpiryDate =  decodedJWT.getExpiresAt();
      Long tokenExpiryTimeInMilliSeconds = tokenExpiryDate.getTime() - (new Date().getTime()) - 60*1000; // minus 60 seconds
      Integer tokenExpiryTime = (int) Math.floor(tokenExpiryTimeInMilliSeconds / 1000); 

      if (tokenExpiryTime <= 0) {
        token = null;
        generateAuthToken();
      }
    }
    return token;
  }

  public void fireEvent(int roundId, String event, String status) throws Exception {
    String token = generateAuthToken();

    Map<String, String> eventConfigs = eventConfig.getEventConfigs();
    
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpPost postRequest = new HttpPost(eventConfigs.get("busApi"));

    postRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.addHeader(HttpHeaders.ACCEPT, "application/json");
    postRequest.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);

    ObjectMapper objectMapper = new ObjectMapper();

    // Generate the payload
    Map<String, Object> payloadMap = new HashMap<String, Object>();
    payloadMap.put("roundId", roundId);
    payloadMap.put("event", event);
    payloadMap.put("status", status);

    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    Instant instant = timestamp.toInstant();

    // Generate the request body to get the token
    Map<String, Object> reqBodyMap = new HashMap<String, Object>();
    reqBodyMap.put("topic", eventConfigs.get("eventTopic"));
    reqBodyMap.put("timestamp", instant.toString());
    reqBodyMap.put("originator", "rating.calculation.service");
    reqBodyMap.put("mime-type",  "application/json");
    reqBodyMap.put("payload",  payloadMap);

    String reqBodyJson = objectMapper.writeValueAsString(reqBodyMap);
    logger.debug("=== sending message to bus ===");

    // post message to the bus to be picked up by the member-profile-processor
    postRequest.setEntity(new StringEntity(reqBodyJson));
    CloseableHttpResponse response = httpClient.execute(postRequest);
    String result = EntityUtils.toString(response.getEntity());

    logger.debug(result);
  }
}
