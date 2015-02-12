package org.onebusaway.prediction;

import org.onebusaway.prediction.service.TestService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class SegmentLevelMeansReportRunner {

    public static void main(String[] args) throws Throwable {
        if(args.length != 2){
            throw new RuntimeException("provide a GTFS trip ID.  Example: CA_A5-Weekday-SDon-084000_X1011_304");
        }
        ConfigurableApplicationContext context = SpringApplication.run(SegmentLevelMeansReportRunner.class, args);
        TestService testService = context.getBean(TestService.class);
        testService.computeStatsForGtfsTripId("CA_A5-Weekday-SDon-084000_X1011_304", "src/main/resources/csv/test_output.csv", false );

        SpringApplication.exit(context);
    }
}