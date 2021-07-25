package com.yyds.ccc.osmtlc.jts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CoordinateProcessor {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(CoordinateProcessor.class);
        CoordinateLocator coordinateLocator = new CoordinateLocator();
//        String DIR = "/home/ec2-user/coordinate-trip-data/yellow_tripdata_2016-total/";
        String[] unprocessedFiles = {
            "part-00046-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00047-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00049-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00051-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00052-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00053-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00054-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00055-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00057-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00058-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00059-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00060-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00061-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00063-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00064-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00065-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00066-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00067-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00068-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00070-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00071-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00072-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00073-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00074-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00075-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00076-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00077-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00078-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00079-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00080-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00081-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00082-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00083-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00084-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00085-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00086-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00087-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00088-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00089-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00090-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00091-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00092-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00093-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00094-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00095-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00096-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00097-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00098-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00099-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00100-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00101-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00102-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00103-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00104-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00105-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00106-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00107-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00108-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00109-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00110-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00111-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00112-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00113-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00114-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00115-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00116-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00117-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00118-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00119-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00120-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00121-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00122-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv",
            "part-00123-8c8838e8-8eb6-48bc-8f86-42b3496ccdca-c000.csv"
        };
//        for (String fileName : unprocessedFiles) {
//            File file = new File(DIR + fileName);
//            logger.info(".......................");
//            logger.info("Processing {}", file.getCanonicalPath());
//            coordinateLocator.process(file);
//            logger.info("Done {}", file.getCanonicalPath());
//        }

        String DIR = "/home/ec2-user/cleaned-nyc-tlc-trip-data/";
//        String DIR = "/Users/yekaiyu/Desktop/CS 598 Cloud Computing Capstone/Research Project/Main";
        File dir = new File(DIR);
        // cleaned-nyc-tlc-trip-data//
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
