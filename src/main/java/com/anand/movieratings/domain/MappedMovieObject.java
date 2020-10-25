package com.anand.movieratings.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MappedMovieObject implements Serializable {
    Integer movieId;
    String movieTitle;
    Integer userId;
    Integer rating;
}
