package org.onebusaway.prediction;

import org.onebusaway.prediction.util.SegmentLevelMahoutPredictionsGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class SegmentLevelPredictionsRunner {

    public static void main(String[] args) throws Throwable {
        if(args.length != 2){
            throw new RuntimeException("provide a GTFS trip ID and a similarity threshold.  Example: CA_A5-Weekday-SDon-084000_X1011_304 100");
        }
        ConfigurableApplicationContext context = SpringApplication.run(SegmentLevelPredictionsRunner.class, args);
        SegmentLevelMahoutPredictionsGenerator segmentLevelMahoutDataGenerator = context.getBean(SegmentLevelMahoutPredictionsGenerator.class);

        Boolean useTravelTime = true;
        Boolean useTimeOfDay = true;

        segmentLevelMahoutDataGenerator.testMahoutRecommendations(args[0], new Double(args[1]), useTravelTime, useTimeOfDay);
        SpringApplication.exit(context);
    }
}