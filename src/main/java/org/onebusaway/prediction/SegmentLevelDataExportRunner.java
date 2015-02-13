package org.onebusaway.prediction;

import org.onebusaway.prediction.util.SegmentLevelDataExporter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class SegmentLevelDataExportRunner {

    public static void main(String[] args) throws Throwable {

        if(args.length != 1){
            throw new RuntimeException("provide a GTFS trip ID.  Example: CA_A5-Weekday-SDon-084000_X1011_304");
        }

        ConfigurableApplicationContext context = SpringApplication.run(SegmentLevelDataExportRunner.class, args);
        SegmentLevelDataExporter segmentLevelDataExporter = context.getBean(SegmentLevelDataExporter.class);

        segmentLevelDataExporter.generateExtract(args[0]);
        SpringApplication.exit(context);
    }
}