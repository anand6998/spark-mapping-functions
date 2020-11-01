package com.anand.movieratings;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.anand.movieratings.domain.MappedMovieObject;
import com.anand.movieratings.domain.MovieDetails;
import com.anand.movieratings.domain.MovieRating;
import com.anand.movieratings.functions.MappingFunctions;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import scala.Int;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class MovieRatingFunctions implements Serializable {
    @Autowired
    SparkConf sparkConf;

    @Autowired
    ResourceLoader resourceLoader;

    @Value("${redis.host.url}")
    String redisHostUrl;

    @Value("${spark.redis.host}")
    String sparkRedisHost;

    @Value("${spark.redis.port}")
    String sparkRedisPort;

    String awsAccessKey;
    String awsSecretKey;

    final static String AWS_ENDPOINT = "s3.amazonaws.com";

    @PostConstruct
    public void init() {
        final AWSCredentialsProvider provider = new SystemPropertiesCredentialsProvider();

        awsAccessKey = provider.getCredentials().getAWSAccessKeyId();
        awsSecretKey = provider.getCredentials().getAWSSecretKey();
    }

    public void testUsingBroadcast() throws Exception {
        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        final SparkContext sparkContext = sparkSession.sparkContext();
        final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkContext);

        sparkContext.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey);
        sparkContext.hadoopConfiguration().set("fs.s3a.secret.key", awsSecretKey);
        sparkContext.hadoopConfiguration().set("fs.s3a.endpoint", AWS_ENDPOINT);

        final Dataset<Row> movieRatingsRawDs = sparkSession.read().text("s3a://aa-movie-ratings/u.data");

        final Dataset<MovieRating> movieRatingDs = movieRatingsRawDs.map(
                MappingFunctions.mapMovieRatingFunction(),
                Encoders.bean(MovieRating.class));

        final Config config = new Config();
        config.useSingleServer().setAddress(redisHostUrl);
        final RedissonClient client = Redisson.create(config);

        final org.redisson.api.RMap<Integer, MovieDetails> movieDetailsRMap = client.getMap("movie_details");

        Set<Map.Entry<Integer, MovieDetails>> entrySet = movieDetailsRMap.entrySet();
        final Map<Integer, MovieDetails> movieDetailsMap = Maps.newHashMap();
        for (Map.Entry<Integer, MovieDetails> entry : entrySet) {
            movieDetailsMap.put(entry.getKey(), entry.getValue());
        }

        //Broadcast the dataset
        final Broadcast<Map<Integer, MovieDetails>> movieDetailsBroadcastMap =
                jsc.broadcast(movieDetailsMap);

        final Dataset<MappedMovieObject> mappedDs = movieRatingDs.map(
                MappingFunctions.mapUsingBroadcastFunction(movieDetailsBroadcastMap),
                Encoders.bean(MappedMovieObject.class)
        );

        mappedDs.createOrReplaceTempView("mappedMovieTable");

        sparkSession.sql("SELECT DISTINCT MovieId, MovieTitle FROM mappedMovieTable").orderBy("movieId").show(false);

        client.shutdown();
        sparkSession.stop();

    }

    public void testNaiveMappingFunction() throws Exception {
        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", awsSecretKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", AWS_ENDPOINT);

        final Dataset<Row> movieRatingsRawDs = sparkSession.read().text("s3a://aa-movie-ratings/u.data");

        final Dataset<MovieRating> movieRatingDs = movieRatingsRawDs.map(
                MappingFunctions.mapMovieRatingFunction(),
                Encoders.bean(MovieRating.class));

        StructType customStructType = new StructType();
        customStructType = customStructType.add("MovieId", DataTypes.IntegerType, false);
        customStructType = customStructType.add("UserId", DataTypes.IntegerType, false);
        customStructType = customStructType.add("MovieRating", DataTypes.IntegerType, false);
        customStructType = customStructType.add("MovieTitle", DataTypes.StringType, false);

        final JavaRDD<MovieRating> movieRatingJavaRDD = movieRatingDs.toJavaRDD();
        final JavaRDD<Row> mappedRDD = movieRatingJavaRDD.mapPartitions(MappingFunctions.movieRatingRowMapFunctionNaive(redisHostUrl));

        final Dataset<Row> mappedDs = sparkSession.createDataFrame(mappedRDD, customStructType);
        mappedDs.createOrReplaceTempView("mappedMovieTable");

        sparkSession.sql("SELECT DISTINCT MovieId, MovieTitle FROM mappedMovieTable").orderBy("movieId").show(false);
        sparkSession.stop();
    }

    public void testMappingFunction() throws Exception {
        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", awsSecretKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", AWS_ENDPOINT);

        final Dataset<Row> movieRatingsRawDs = sparkSession.read().text("s3a://aa-movie-ratings/u.data");

        final Dataset<MovieRating> movieRatingDs = movieRatingsRawDs.map(
                MappingFunctions.mapMovieRatingFunction(),
                Encoders.bean(MovieRating.class));

        StructType customStructType = new StructType();
        customStructType = customStructType.add("MovieId", DataTypes.IntegerType, false);
        customStructType = customStructType.add("UserId", DataTypes.IntegerType, false);
        customStructType = customStructType.add("MovieRating", DataTypes.IntegerType, false);
        customStructType = customStructType.add("MovieTitle", DataTypes.StringType, false);

        final JavaRDD<MovieRating> movieRatingJavaRDD = movieRatingDs.toJavaRDD();
        final JavaRDD<Row> mappedRDD = movieRatingJavaRDD.mapPartitions(MappingFunctions.movieRatingRowMapFunction(redisHostUrl));

        final Dataset<Row> mappedDs = sparkSession.createDataFrame(mappedRDD, customStructType);
        mappedDs.createOrReplaceTempView("mappedMovieTable");

        sparkSession.sql("SELECT DISTINCT MovieId, MovieTitle FROM mappedMovieTable").orderBy("movieId").show(false);
        sparkSession.stop();

    }

    public void testWithInmemoryHashMap() throws Exception {
        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .config("spark.redis.host", sparkRedisHost)
                .config("spark.redis.port", sparkRedisPort)
                .getOrCreate();

        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", awsSecretKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", AWS_ENDPOINT);

        final Dataset<Row> movieRatingsRawDs = sparkSession.read().text("s3a://aa-movie-ratings/u.data");

        final Dataset<MovieRating> movieRatingDs = movieRatingsRawDs.map(
                MappingFunctions.mapMovieRatingFunction(),
                Encoders.bean(MovieRating.class));

        final Dataset<MovieDetails> movieDetailsDs = sparkSession.read()
                .format("org.apache.spark.sql.redis")
                .option("table", "tblMovieDetails")
                .option("key.column", "movieId")
                .load()
                .as(Encoders.bean(MovieDetails.class));

        final java.util.List<MovieRating> movieRatings = movieRatingDs.collectAsList();
        final java.util.List<MovieDetails> movieDetails = movieDetailsDs.collectAsList();

        final Map<Integer, MovieDetails> movieDetailsMap = movieDetails.stream().collect(Collectors.toMap(x -> x.getMovieId(), x -> x));

        System.out.println(movieRatings.size());
        System.out.println(movieDetails.size());

        final Map<Integer, String> movieObjectMap = Maps.newHashMap();

        for (MovieRating movieRating : movieRatings) {
            final Integer movieId = movieRating.getMovieId();

            String movieTitle = "N/A";
            if (movieDetailsMap.containsKey(movieId)) {
                movieTitle = movieDetailsMap.get(movieId).getMovieTitle();
            }

            movieObjectMap.put(movieId, movieTitle);
        }

        List<Map.Entry<Integer, String>> mapList = new LinkedList<>(movieObjectMap.entrySet());
        Collections.sort(mapList,
                new Comparator<Map.Entry<Integer, String>>() {
                    @Override
                    public int compare(Map.Entry<Integer, String> o1, Map.Entry<Integer, String> o2) {
                        return o1.getKey().compareTo( o2.getKey());
                    }
                });

        int i = 20;
        for (Map.Entry<Integer, String> entry: mapList) {
            //resultMap.put(entry.getKey(), entry.getValue());
            System.out.println(entry.getKey() + "| " + entry.getValue());
            i -= 1;
            if (i == 0) {
                break;
            }
        }
        sparkSession.stop();


    }

    public void testMappingFunctionWithDatasets() throws Exception {

        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .config("spark.redis.host", sparkRedisHost)
                .config("spark.redis.port", sparkRedisPort)
                .getOrCreate();

        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", awsSecretKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", AWS_ENDPOINT);

        final Dataset<Row> movieRatingsRawDs = sparkSession.read().text("s3a://aa-movie-ratings/u.data");

//      Read movie details from Redis
        final Dataset<MovieRating> movieRatingDs = movieRatingsRawDs.map(
                MappingFunctions.mapMovieRatingFunction(),
                Encoders.bean(MovieRating.class));

        final Dataset<MovieDetails> movieDetailsDs = sparkSession.read()
                .format("org.apache.spark.sql.redis")
                .option("table", "tblMovieDetails")
                .option("key.column", "movieId")
                .load()
                .as(Encoders.bean(MovieDetails.class));

        movieDetailsDs.createOrReplaceTempView("movie_details");
        movieRatingDs.createOrReplaceTempView("movie_ratings");

        sparkSession.sql("SELECT distinct t1.movieId, t2.movieTitle" +
                " FROM movie_ratings t1 INNER JOIN movie_details t2" +
                " ON t1.movieId = t2.movieId").orderBy("movieId").show(false);



        sparkSession.stop();
    }
}
