package com.anand.movieratings.domain;

import java.io.Serializable;

public class MovieRating implements Serializable {

    //user_id | item_id | rating | timestamp
    Integer userId;
    Integer movieId;
    Integer rating;

    public MovieRating() {}

    public MovieRating(Integer userId, Integer movieId, Integer rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }
}
