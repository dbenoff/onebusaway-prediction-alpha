package org.onebusaway.prediction;

import org.onebusaway.prediction.util.GTFSImporter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class GtfsImportRunner {

    public static void main(String[] args) throws Throwable {
        if(args.length != 1){
            throw new RuntimeException("Specify a path to your untar'ed GTFS directory, example: ~/mygtfsdir");
        }
        ConfigurableApplicationContext context = SpringApplication.run(GtfsImportRunner.class, args);
        GTFSImporter gtfsImporter = context.getBean(GTFSImporter.class);
        gtfsImporter.importGtfs(args[0]);
        SpringApplication.exit(context);
    }
}