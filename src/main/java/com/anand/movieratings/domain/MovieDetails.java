package com.anand.movieratings.domain;

import java.io.Serializable;

public class MovieDetails implements Serializable {

    Integer movieId;
    String movieTitle;
    java.sql.Date releaseDate;


    @Override
    public String toString() {
        return "MovieDetails{" +
                "movieId=" + movieId +
                ", movieTitle='" + movieTitle + '\'' +
                ", releaseDate=" + releaseDate +
                '}';
    }

    public MovieDetails() {}

    public MovieDetails(Integer movieId, String movieTitle, java.sql.Date releaseDate) {
        this.movieId = movieId;
        this.movieTitle = movieTitle;
        this.releaseDate = releaseDate;

    }

    public MovieDetails(Builder builder) {
        this.movieId = builder.movieId;
        this.movieTitle = builder.movieTitle;
        this.releaseDate = builder.releaseDate;
    }

    public java.sql.Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(java.sql.Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public String getMovieTitle() {
        return movieTitle;
    }

    public void setMovieTitle(String movieTitle) {
        this.movieTitle = movieTitle;
    }


    public static final class Builder {
        private Integer movieId;
        private String movieTitle;
        private java.sql.Date releaseDate;

        public Builder setMovieId(Integer movieId) {
            this.movieId = movieId;
            return this;
        }

        public Builder setMovieTitle(String movieTitle) {
            this.movieTitle = movieTitle;
            return this;
        }

        public MovieDetails build() {
            return new MovieDetails(this);
        }

        public Builder setReleaseDate(java.sql.Date releaseDate) {
            this.releaseDate = releaseDate;
            return this;
        }
    }
}
