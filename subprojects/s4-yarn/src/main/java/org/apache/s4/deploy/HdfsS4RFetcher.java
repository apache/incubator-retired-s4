package org.apache.s4.deploy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Fetches S4R archives from HDFS, i.e. S4R published with a "hdfs://path/to/the/file" - like URI.
 * 
 * The HDFS configuration is fetched from the classpath (typically, from core-site.xml).
 * 
 */
public class HdfsS4RFetcher implements S4RFetcher {

    private static Logger logger = LoggerFactory.getLogger(HdfsS4RFetcher.class);

    final Configuration conf;

    // the fs name holder is optional and only used by tests. Default value is null
    static class FsNameHolder {
        @Inject(optional = true)
        @Named(FileSystem.FS_DEFAULT_NAME_KEY)
        String value = null;
    }

    @Inject
    public HdfsS4RFetcher(FsNameHolder fsNameHolder) {
        this.conf = new YarnConfiguration();
        if (fsNameHolder.value != null) {
            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fsNameHolder.value);
        }
    }

    @Override
    public boolean handlesProtocol(URI uri) {
        return "hdfs".equalsIgnoreCase(uri.getScheme());
    }

    @Override
    public InputStream fetch(URI uri) throws DeploymentFailedException {
        try {
            logger.info("Fetching S4R through hdfs from uri {}", uri.toString());
            FileSystem fs = FileSystem.get(conf);
            Path s4rPath = new Path(uri);
            if (!fs.exists(s4rPath)) {
                fs.close();
                throw new DeploymentFailedException("Cannot find S4R file at URI : " + uri.toString());
            }
            return fs.open(s4rPath);
        } catch (IOException e) {
            throw new DeploymentFailedException("Cannot fetch S4R file at URI: " + uri.toString(), e);
        }
    }
}
