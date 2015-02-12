# 

## Getting Started

### Database setup

Create a new MySQL database, modify application.properties to point to your new database.

Run predictions_schema.sql to create the tables.

Import the OBA dump of obanyc_inferredlocation table.

Insert the desired test records into inferredlocation table.

### Import GTFS data

Now you need to import GTFS data.

Untar a bundle to your local drive.  Run GTFSImportRunner, supplying the path to the dir you untar'ed.  The app will traverse the directory structure looking for zipped GTFS files and will import them into your local DB.

### Generate arrivals data

In this step, we read through each of the records in inferredlocation and try to figure out when a bus arrived at a stop, then persist the arrival.

Run PopulateArrivalsRunner with no args.  This can take a VERY long time to run.  Also note that any existing arrivals data in the DB will be lost.

### Generate arrivals data

