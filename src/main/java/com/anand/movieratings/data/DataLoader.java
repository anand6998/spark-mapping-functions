package com.anand.movieratings.data;

import com.anand.movieratings.MovieRatingFunctions;
import com.anand.movieratings.domain.MovieDetails;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

@Component
public class DataLoader {

    @Autowired
    ResourceLoader resourceLoader;

    @Autowired
    SparkConf sparkConf;

    @Value("${redis.host.url}")
    String redisHostUrl;

    @Value("${spark.redis.host}")
    String sparkRedisHost;

    @Value("${spark.redis.port}")
    String sparkRedisPort;


    public void loadDataAsSparkTable() throws Exception {
        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                //connection parameters for Redis
                .config("spark.redis.host", sparkRedisHost)
                .config("spark.redis.port", sparkRedisPort)
                .getOrCreate();

        final String movieDetailsFile =
                resourceLoader.getResource("classpath:/data/movieratings/u.item").getFile().getAbsolutePath();

        final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        final JavaRDD<MovieDetails> movieDetailsRDD = jsc.textFile(movieDetailsFile)
                .map((Function<String, MovieDetails>) s -> {

                    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d-MMM-yyyy");
                    final String[] details = s.split("\\|");
                    try {
                        final Integer movieId = Integer.parseInt(details[0]);
                        final String movieName = details[1];
                        final Date releaseDate = Date.valueOf(LocalDate.parse(details[2], formatter));
                        final MovieDetails movieDetails = new MovieDetails(movieId, movieName, releaseDate);
                        return movieDetails;
                    } catch (Exception ex) {
                        //ex.printStackTrace();
                        return null;
                    }
                }).filter((Function<MovieDetails, Boolean>) movieDetails -> Objects.nonNull(movieDetails));
        System.out.println(movieDetailsRDD.count());

        final Dataset<MovieDetails> movieDetailsDataset = sparkSession.createDataset(movieDetailsRDD.rdd(),
                Encoders.bean(MovieDetails.class));

        movieDetailsDataset.printSchema();


//    write this data set to Redis
        System.out.println("Writing out to Redis");
        movieDetailsDataset.write()
                .format("org.apache.spark.sql.redis")
                .option("table", "tblMovieDetails")
                .option("key.column", "movieId")
                .mode(SaveMode.Overwrite)
                .save();
        sparkSession.close();
        sparkSession.stop();
    }

    public void loadDataInMap() {
        final Config config = new Config();
        config.useSingleServer().setAddress(redisHostUrl);

        final RedissonClient client = Redisson.create(config);

        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        try {


            final String movieDetailsFile =
                    resourceLoader.getResource("classpath:/data/movieratings/u.item").getFile().getAbsolutePath();

            final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
            final JavaRDD<MovieDetails> movieDetailsRDD = jsc.textFile(movieDetailsFile)
                    .map((Function<String, MovieDetails>) s -> {
                        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d-MMM-yyyy");
                        final String[] details = s.split("\\|");
                        try {
                            final Integer movieId = Integer.parseInt(details[0]);
                            final String movieName = details[1];
                            final Date releaseDate = Date.valueOf(LocalDate.parse(details[2], formatter));
                            final MovieDetails movieDetails = new MovieDetails(movieId, movieName, releaseDate);
                            return movieDetails;
                        } catch (Exception ex) {
                            //ex.printStackTrace();
                            return null;
                        }
                    }).filter((Function<MovieDetails, Boolean>) movieDetails -> Objects.nonNull(movieDetails));
            System.out.println(movieDetailsRDD.count());

            final Dataset<MovieDetails> movieDetailsDataset = sparkSession.createDataset(movieDetailsRDD.rdd(),
                    Encoders.bean(MovieDetails.class));

            final List<MovieDetails> movieDetailsList = movieDetailsDataset.collectAsList();

            final RMap map = client.getMap("movie_details");
            for (MovieDetails md : movieDetailsList) {
                map.put(md.getMovieId(), md);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            client.shutdown();
            sparkSession.close();
            sparkSession.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");
        final DataLoader dataLoader = (DataLoader) ctx.getBean("dataLoader");
        dataLoader.loadDataAsSparkTable();
        dataLoader.loadDataInMap();
    }
}
