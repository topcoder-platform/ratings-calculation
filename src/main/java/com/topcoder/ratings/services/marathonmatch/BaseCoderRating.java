// package com.topcoder.ratings.services.marathonmatch;

// public class BaseCoderRating implements Comparable {
//   private int _coderId = 0;
//   private int _rating = 0;

//   BaseCoderRating(int coderId, int rating) {
//     _coderId = coderId;
//     _rating = rating;
//   }

//   public int compareTo(Object other) {
//     if (((CoderRating) other).getRating() > _rating)
//       return 1;
//     else if (((CoderRating) other).getRating() < _rating)
//       return -1;
//     else
//       return 0;
//   }

//   int getCoderId() {
//     return _coderId;
//   }

//   int getRating() {
//     return _rating;
//   }

//   void setCoderId(int coderId) {
//     _coderId = coderId;
//   }

//   void setRating(int rating) {
//     _rating = rating;
//   }

//   public String toString() {
//     return new String(_coderId + ":" + _rating);
//   }
// }