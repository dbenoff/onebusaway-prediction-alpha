package org.onebusaway.prediction;

import org.onebusaway.prediction.entities.Trip;
import org.onebusaway.prediction.service.TripDataService;
import org.onebusaway.prediction.util.GTFSImporter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.Collections;

@Configuration
@ComponentScan
@EnableAutoConfiguration
public class PopulateArrivalsRunner {

    public static void main(String[] args) throws Throwable {
        ConfigurableApplicationContext context = SpringApplication.run(PopulateArrivalsRunner.class, args);
        TripDataService tripDataService = context.getBean(TripDataService.class);
        tripDataService.truncateArrivals();
        Collection<String> tripIds = tripDataService.getTripIds();
        for(String tripId : tripIds){
            Collection<Trip> trips = tripDataService.getTrips(tripId);
            tripDataService.persistArrivals(trips);
        }
        SpringApplication.exit(context);
    }
}