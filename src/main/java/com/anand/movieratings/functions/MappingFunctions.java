package com.anand.movieratings.functions;

import com.anand.movieratings.domain.MappedMovieObject;
import com.anand.movieratings.domain.MovieDetails;
import com.anand.movieratings.domain.MovieRating;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.sources.In;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MappingFunctions {

    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("d-MMM-yyyy");
    public static MapFunction<Row, MovieRating> mapMovieRatingFunction () {
        MapFunction<Row, MovieRating> function = new MapFunction<Row, MovieRating>() {
            @Override
            public MovieRating call(Row row) throws Exception {
                final String rowString = row.get(0).toString();
                final String[] rowValues = rowString.split("\t");
                final MovieRating movieRating = new MovieRating(
                        Integer.parseInt(rowValues[0]), //userId
                        Integer.parseInt(rowValues[1]), //movieId
                        Integer.parseInt(rowValues[2]) //rating
                );
                return movieRating;
            }
        };
        return function;
    }

    public static FlatMapFunction<Iterator<MovieRating>, Row> movieRatingRowMapFunctionNaive(String redisHostUrl) {
        return new FlatMapFunction<Iterator<MovieRating>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<MovieRating> movieRatingIterator) throws Exception {
                final Config config = new Config();
                config.useSingleServer().setAddress(redisHostUrl);
                final RedissonClient client = Redisson.create(config);
                final List<Row> rowList = Lists.newArrayList();
                try {
                    final RMap<Integer, MovieDetails> movieDetailsRMap = client.getMap("movie_details");
                    final Map<Integer, List<MovieRating>> movieRatingsMap = Maps.newHashMap();

                    while (movieRatingIterator.hasNext()) {
                        final MovieRating movieRating = movieRatingIterator.next();
                        final Integer movieId = movieRating.getMovieId();
                        final MovieDetails movieDetails = movieDetailsRMap.get(movieId);
                        String movieTitle = "N/A";
                        if (Objects.nonNull(movieDetails)) {
                            movieTitle = movieDetails.getMovieTitle();
                        }

                        final Integer userId = movieRating.getUserId();
                        final Integer rating = movieRating.getRating();

                        final Row row = RowFactory.create(movieId, userId, rating, movieTitle);
                        rowList.add(row);

                    }

                    return rowList.iterator();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    List<Row> list = Lists.newArrayList();
                    return list.iterator();
                } finally {
                    client.shutdown();
                }

            }
        };
    }

    public static FlatMapFunction<Iterator<MovieRating>, Row> movieRatingRowMapFunction(String redisHostUrl) {
        return new FlatMapFunction<Iterator<MovieRating>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<MovieRating> movieRatingIterator) throws Exception {
                final Config config = new Config();
                config.useSingleServer().setAddress(redisHostUrl);
                final RedissonClient client = Redisson.create(config);
                final List<Row> rowList = Lists.newArrayList();
                try {
                    final RMap<Integer, MovieDetails> movieDetailsRMap = client.getMap("movie_details");
                    final Map<Integer, List<MovieRating>> movieRatingsMap = Maps.newHashMap();

                    while (movieRatingIterator.hasNext()) {
                        final MovieRating movieRating = movieRatingIterator.next();
                        List<MovieRating> movieRatingList = movieRatingsMap.get(movieRating.getMovieId());
                        if(Objects.isNull(movieRatingList)) {
                            movieRatingList = Lists.newArrayList();
                            movieRatingsMap.put(movieRating.getMovieId(), movieRatingList);
                        }
                        movieRatingList.add(movieRating);
                    }

                    final List<Integer> movieIdList = Lists.newArrayList(movieRatingsMap.keySet());

                    for (Integer movieId : movieIdList) {
                        final List<MovieRating> list = movieRatingsMap.get(movieId);
                        final MovieDetails movieDetails = movieDetailsRMap.get(movieId);
                        String movieTitle = "N/A";
                        if (Objects.nonNull(movieDetails)) {
                            movieTitle = movieDetails.getMovieTitle();
                        }

                        for (MovieRating movieRating : list) {
                            final Integer userId = movieRating.getUserId();
                            final Integer rating = movieRating.getRating();
                            final Row row = RowFactory.create(movieId, userId, rating, movieTitle);
                            rowList.add(row);
                        }
                    }

                    return rowList.iterator();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    final List<Row> list = Lists.newArrayList();
                    return list.iterator();
                } finally {
                    client.shutdown();
                }

            }
        };
    }

    public static MapFunction<Row, MovieDetails> mapMovieDetailsFunction () {
        MapFunction<Row, MovieDetails> function = new MapFunction<Row, MovieDetails>() {
            @Override
            public MovieDetails call(Row row) throws Exception {
                final String rowString = row.get(0).toString();
                final String[] rowValues = rowString.split("\\|");

                final Integer movieId = Integer.parseInt( rowValues[0]);
                final String movieTitle = rowValues[1];
                final LocalDate releaseDt = parseDate(rowValues[2]);
                java.sql.Date releaseDate = null;
                if (Objects.nonNull(releaseDt)) {
                    releaseDate = java.sql.Date.valueOf(releaseDt);
                } else {
                    releaseDate = null;
                }

                final MovieDetails.Builder builder = new MovieDetails.Builder()
                        .setMovieId(movieId)
                        .setMovieTitle(movieTitle)
                        .setReleaseDate(releaseDate);

                MovieDetails movieDetails = builder.build();
                return movieDetails;
            }
        };
        return function;
    }

    public static LocalDate parseDate(String value) {
        if (Objects.isNull(value) || value.trim().equals("")) {
            return null;
        }

        return LocalDate.parse(value, dateTimeFormatter);
    }

    public static MapFunction<MovieRating, MappedMovieObject> mapUsingBroadcastFunction(Broadcast<Map<Integer, MovieDetails>> movieDetailsBroadcastMap) {

        final MapFunction<MovieRating, MappedMovieObject> function = (MapFunction<MovieRating, MappedMovieObject>) movieRating -> {
            final Map<Integer, MovieDetails> movieDetailsRMap = movieDetailsBroadcastMap.getValue();

            final Integer movieId = movieRating.getMovieId();
            String movieTitle = "N/A";
            final MovieDetails movieDetails = movieDetailsRMap.get(movieId);
            if (Objects.nonNull(movieDetails)) {
                movieTitle = movieDetails.getMovieTitle();
            }
            final Integer userId = movieRating.getUserId();
            final Integer rating = movieRating.getRating();


            final MappedMovieObject mappedMovieObject = new MappedMovieObject(movieId, movieTitle, userId, rating);
            return mappedMovieObject;
        };

        return function;

    }
}
