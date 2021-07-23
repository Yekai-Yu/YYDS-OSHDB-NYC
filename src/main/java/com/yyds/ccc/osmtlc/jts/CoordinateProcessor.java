package com.yyds.ccc.osmtlc.jts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CoordinateProcessor {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(CoordinateProcessor.class);
        CoordinateLocator coordinateLocator = new CoordinateLocator();
        String DIR = "home/ec2-user/coordinate-trip-data/";
        File dir = new File(DIR);
        // coordinate-trip-data/
        File[] dirs = dir.listFiles();
        for (File subDir : dirs) {
            // green_tripdata_2014-total/
            logger.info("=====================================");
            logger.info("In {}", subDir.getCanonicalPath());
            if (subDir.isDirectory()) {
                File[] files = subDir.listFiles();
                for (File file : files) {
                    if (file.isFile() && file.getAbsolutePath().endsWith(".csv")) {
                        logger.info(".......................");
                        logger.info("Processing {}", file.getCanonicalPath());
                        coordinateLocator.process(file);
                        logger.info("Done {}", file.getCanonicalPath());
                    }
                }
            }
        }
        logger.info("Complete.");
    }
}
