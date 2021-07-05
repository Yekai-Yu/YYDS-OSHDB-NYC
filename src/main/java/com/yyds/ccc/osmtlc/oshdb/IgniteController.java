package com.yyds.ccc.osmtlc.oshdb;

import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RestController
@RequestMapping(value = "/ignite")
public class IgniteController extends POCController {
    Logger logger = LoggerFactory.getLogger(POCController.class);

    Ignite ignite;
    OSHDBDatabase oshdb;
    OSHDBJdbc keytables = null;

    @RequestMapping(value = "/testIg", method = RequestMethod.GET)
    public void test() throws Exception {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        BasicAWSCredentials creds = new BasicAWSCredentials("AKIA3HHI5KS4Y6WR6POL", "0TxOTY36rNNw07h3WqXXsAxCCvXLe7UVO7YLFWLt");

        TcpDiscoveryS3IpFinder ipFinder = new TcpDiscoveryS3IpFinder();
        ipFinder.setAwsCredentials(creds);
        ipFinder.setBucketName("ignite-bucket-ccc");

        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setIgniteInstanceName("OSHDB-Unit-Tests_" + 8080);
        cfg.setBinaryConfiguration((new BinaryConfiguration()).setCompactFooter(false));
        cfg.setGridLogger(new Slf4jLogger());
        cfg.setWorkDirectory("/tmp");
        cfg.setClientMode(true);

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start a node.
        ignite = Ignition.start(cfg);

        oshdb = new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.LocalPeek);
        final String prefix = "tests";
        oshdb.prefix(prefix);

        OSHDBH2 oshdbH2 = new OSHDBH2(DB_PATH);
        this.keytables = oshdbH2;

        Ignite ignite = ((OSHDBIgnite) this.oshdb).getIgnite();
        ignite.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Long, GridOSHNodes> cacheCfg =
                new CacheConfiguration<>(TableNames.T_NODES.toString(prefix));
        cacheCfg.setStatisticsEnabled(true);
        cacheCfg.setBackups(0);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        IgniteCache<Long, GridOSHNodes> cache = ignite.getOrCreateCache(cacheCfg);
        cache.clear();
        // dummy caches for ways+relations (at the moment we don't use them in the actual TestMapReduce)
        ignite.getOrCreateCache(new CacheConfiguration<>(TableNames.T_WAYS.toString(prefix)));
        ignite.getOrCreateCache(new CacheConfiguration<>(TableNames.T_RELATIONS.toString(prefix)));

        // load test data into ignite cache
        try (IgniteDataStreamer<Long, GridOSHNodes> streamer = ignite.dataStreamer(cache.getName())) {
            Connection h2Conn = oshdbH2.getConnection();
            Statement h2Stmt = h2Conn.createStatement();

            streamer.allowOverwrite(true);

            try (final ResultSet rst =
                         h2Stmt.executeQuery("select level, id, data from " + TableNames.T_NODES.toString())) {
                while (rst.next()) {
                    final int level = rst.getInt(1);
                    final long id = rst.getLong(2);
                    final ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(3));
                    final GridOSHNodes grid = (GridOSHNodes) ois.readObject();

                    streamer.addData(CellId.getLevelId(level, id), grid);
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();

            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }

        ignite.cluster().state(ClusterState.ACTIVE_READ_ONLY);

        logger.info("Init TagTranslator");
        tagTranslator = new TagTranslator(oshdbH2.getConnection());
        logger.info("Init view");
        view = OSMEntitySnapshotView.on(oshdb);

        super.poc();
    }

}
