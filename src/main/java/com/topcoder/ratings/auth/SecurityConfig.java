package com.topcoder.ratings.auth;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;
import org.springframework.security.oauth2.server.resource.authentication.JwtIssuerAuthenticationManagerResolver;
import org.springframework.security.web.SecurityFilterChain;

import com.topcoder.ratings.security.TopcoderTokenValidator;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ratings")
@EnableWebSecurity
public class SecurityConfig {
  
	private String validIssuers[];

	private Map<String, Map<String, String>> tokenPropertiesToValidate;

	private Map<String, AuthenticationManager> authenticationManagers = new HashMap<>();

	public String[] getValidIssuers() {
		return validIssuers;
	}

	public void setValidIssuers(String[] validIssuers) {
		this.validIssuers = validIssuers;
	}

	public Map<String, Map<String, String>> getTokenPropertiesToValidate() {
		return tokenPropertiesToValidate;
	}

	public void setTokenPropertiesToValidate(Map<String, Map<String, String>> tokenPropertiesToValidate) {
		this.tokenPropertiesToValidate = tokenPropertiesToValidate;
	}


  // the main configuration for the security filter
  @Bean
  SecurityFilterChain ratingsAPIFilterChain(HttpSecurity http) throws Exception {
		// @formatter:off
		http.
		sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
		.and()
		.authorizeHttpRequests()
				.requestMatchers("/ratings/health").permitAll()
        .requestMatchers("/ratings/**").authenticated()
				.and().cors()
				.and().csrf()
				.and().oauth2ResourceServer()
				.authenticationManagerResolver(jwtIssuerAuthenticationManagerResolver());

		return http.build();
		// @formatter:on
	}

  @Bean
  JwtIssuerAuthenticationManagerResolver jwtIssuerAuthenticationManagerResolver() {

    for (String issuer : validIssuers) {
      JwtAuthenticationProvider jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtDecoder(issuer));
      authenticationManagers.put(issuer, jwtAuthenticationProvider::authenticate);
    }
    return new JwtIssuerAuthenticationManagerResolver(authenticationManagers::get);
	}

	private JwtDecoder jwtDecoder(String issuer) {

		NimbusJwtDecoder jwtDecoder = (NimbusJwtDecoder) JwtDecoders.fromOidcIssuerLocation(issuer);

		OAuth2TokenValidator<Jwt> tokenValidator = new TopcoderTokenValidator(tokenPropertiesToValidate,issuer);
		OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
		OAuth2TokenValidator<Jwt> withTokenValidator = new DelegatingOAuth2TokenValidator<>(withIssuer, tokenValidator);

		jwtDecoder.setJwtValidator(withTokenValidator);

		return jwtDecoder;
	}
}
