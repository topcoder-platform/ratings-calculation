package com.topcoder.ratings.services;

public class CoderRating implements Comparable<BaseCoderRating> {
  long coderId = 0;
  int rating = 0;
  long schoolId = 0;
  boolean active = false;
  String countryCode = null;
  String stateCode = null;

  CoderRating(long coderId, int rating, long schoolId, boolean active, String countryCode, String stateCode) {
    this.coderId = coderId;
    this.rating = rating;
    this.schoolId = schoolId;
    this.active = active;
    this.countryCode = countryCode;
    this.stateCode = stateCode;
  }

  public int compareTo(BaseCoderRating other) {
    if (other.getRating() > rating)
      return 1;
    else if (other.getRating() < rating)
      return -1;
    else
      return 0;
  }

  long getCoderId() {
    return coderId;
  }

  int getRating() {
    return rating;
  }

  void setCoderId(long coderId) {
    this.coderId = coderId;
  }

  void setRating(int rating) {
    this.rating = rating;
  }

  long getSchoolId() {
    return schoolId;
  }

  void setSchoolId(long schoolId) {
    this.schoolId = schoolId;
  }

  boolean isActive() {
    return active;
  }

  void setActive(boolean active) {
    this.active = active;
  }

  String getStateCode() {
    return stateCode;
  }

  void setStateCode(String stateCode) {
    this.stateCode = stateCode;
  }

  String getCountryCode() {
    return countryCode;
  }

  void setCountryCode(String countryCode) {
    this.countryCode = countryCode;
  }

  public String toString() {
    return new String(coderId + ":" + rating + ":" + schoolId + ":" + active + ":" + stateCode + ":" + countryCode);
  }
}
