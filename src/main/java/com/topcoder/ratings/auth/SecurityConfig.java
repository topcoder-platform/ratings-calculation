package com.topcoder.ratings.auth;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;
import org.springframework.security.oauth2.server.resource.authentication.JwtIssuerAuthenticationManagerResolver;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;

import com.topcoder.ratings.security.RatingsApiAccessDeniedHandler;
import com.topcoder.ratings.security.RatingsApiAuthenticationEntryPoint;
import com.topcoder.ratings.security.RatingsApiJwtGrantedAuthoritiesConverter;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ratings")
@EnableWebSecurity
public class SecurityConfig {

	@Value("${springdoc.api-docs.path}")
	private String apiDocPath;
  
	private String authorityPrefix;

	private String allowedAuthority[];

	private String validIssuers[];

	private String authorityClaims[];

	private Map<String, AuthenticationManager> authenticationManagers = new HashMap<>();

	public String getAuthorityPrefix() {
		return authorityPrefix;
	}

	public void setAuthorityPrefix(String authorityPrefix) {
		this.authorityPrefix = authorityPrefix;
	}

	public String[] getAllowedAuthority() {
		return allowedAuthority;
	}

	public void setAllowedAuthority(String[] allowedAuthority) {
		this.allowedAuthority = allowedAuthority;
	}

	public String[] getValidIssuers() {
		return validIssuers;
	}

	public void setValidIssuers(String[] validIssuers) {
		this.validIssuers = validIssuers;
	}

	public String[] getAuthorityClaims() {
		return authorityClaims;
	}

	public void setAuthorityClaims(String[] authorityClaims) {
		this.authorityClaims = authorityClaims;
	}


  // the main configuration for the security filter
  @Bean
  SecurityFilterChain ratingsAPIFilterChain(HttpSecurity http) throws Exception {
		// @formatter:off
		http
		.cors(Customizer.withDefaults())
		.csrf(Customizer.withDefaults())
		.authorizeHttpRequests(authorize -> authorize
				.requestMatchers(apiDocPath).permitAll()
				.requestMatchers("/v5/ratings/health").permitAll()
				.requestMatchers("/v5/ratings/mm/**").hasAnyAuthority(allowedAuthority)
				.requestMatchers("/v5/ratings/coders/**").hasAnyAuthority(allowedAuthority))
		.oauth2ResourceServer(oauth2 -> oauth2
				.authenticationManagerResolver(jwtIssuerAuthenticationManagerResolver())
				.accessDeniedHandler(accessDeniedHandler())
				.authenticationEntryPoint(authenticationEntryPoint()))
		.sessionManagement(session -> session
				.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
		// @formatter:on

		return http.build();
	}

	// custom 403 Forbidden error handler
	@Bean
	AccessDeniedHandler accessDeniedHandler() {
		return new RatingsApiAccessDeniedHandler();
	}

	// custom 401 Unauthorized error handler
	@Bean
	AuthenticationEntryPoint authenticationEntryPoint() {
		return new RatingsApiAuthenticationEntryPoint();
	}

  @Bean
	JwtIssuerAuthenticationManagerResolver jwtIssuerAuthenticationManagerResolver() {

		for (String issuer : this.validIssuers) {
			JwtAuthenticationProvider jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtDecoder(issuer));
			jwtAuthenticationProvider.setJwtAuthenticationConverter(customJwtAuthenticationConverter(issuer));
			authenticationManagers.put(issuer, jwtAuthenticationProvider::authenticate);

		}
		return new JwtIssuerAuthenticationManagerResolver(authenticationManagers::get);
	}

	private Converter<Jwt, Collection<GrantedAuthority>> customJwtGrantedAuthoritiesConverter() {

		RatingsApiJwtGrantedAuthoritiesConverter jwtGrantedAuthoritiesConverter = new RatingsApiJwtGrantedAuthoritiesConverter();

		System.out.println(this.authorityPrefix);
		System.out.println(this.authorityClaims);
		
		jwtGrantedAuthoritiesConverter.setAuthorityPrefix(this.authorityPrefix);
		jwtGrantedAuthoritiesConverter.setAuthorityClaims(this.authorityClaims);
		return jwtGrantedAuthoritiesConverter;
	}

	private JwtAuthenticationConverter customJwtAuthenticationConverter(String issuer) {

		JwtAuthenticationConverter jwtAuthenticationConverter = new JwtAuthenticationConverter();
		jwtAuthenticationConverter.setJwtGrantedAuthoritiesConverter(customJwtGrantedAuthoritiesConverter());

		return jwtAuthenticationConverter;
	}

	private JwtDecoder jwtDecoder(String issuer) {

		NimbusJwtDecoder jwtDecoder = (NimbusJwtDecoder) JwtDecoders.fromOidcIssuerLocation(issuer);

		OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
		OAuth2TokenValidator<Jwt> withTokenValidator = new DelegatingOAuth2TokenValidator<>(withIssuer);

		jwtDecoder.setJwtValidator(withTokenValidator);

		return jwtDecoder;
	}
}
