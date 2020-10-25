package com.anand.movieratings;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;


public class MovieRatingsMain {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");
        final MovieRatingFunctions movieRatingFunctions = (MovieRatingFunctions) ctx.getBean("movieRatingFunctions");



        Stopwatch timer = Stopwatch.createUnstarted();

        timer.start();
        movieRatingFunctions.testUsingBroadcast();
        long timeUsingBroadcast = timer.elapsed(TimeUnit.MILLISECONDS);
        timer.reset();

        timer.start();
        movieRatingFunctions.testMappingFunction();

        long timeUsingIntelligentMapping = timer.elapsed(TimeUnit.MILLISECONDS);
        timer.reset();

        timer.start();
        movieRatingFunctions.testMappingFunctionWithDatasets();
        long timeUsingDatasets = timer.elapsed(TimeUnit.MILLISECONDS);

        timer.reset();

        timer.start();
        movieRatingFunctions.testNaiveMappingFunction();
        long timeUsingNaiveMapping = timer.elapsed(TimeUnit.MILLISECONDS);

        System.out.println("Time using intelligent mapping : " + timeUsingIntelligentMapping);
        System.out.println("Time using naive mapping : " + timeUsingNaiveMapping);
        System.out.println("Time using datasets : " + timeUsingDatasets);
        System.out.println("Time using broadcast : " + timeUsingBroadcast);
        timer.reset();


    }
}
