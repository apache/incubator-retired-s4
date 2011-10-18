# Manually packaging an S4 app.

The app should extend io.s4.core.App:

<pre>
import io.s4.core.App;


public class HelloApp extends App {

    @Override
    protected void start() {
        System.out.println("Hello App! I'm starting...");        
    }

    @Override
    protected void init() {
        System.out.println("Hello App! I'm initing...");        
        
    }

    @Override
    protected void close() {
        System.out.println("Hello App! I'm closing...");        
        
    }
}
</pre>

We want to indicate the entry point in the jar. To do this I create a manifest file.

* Edit MANIFEST.MF:

<pre>
Manifest-Version: 1.0
provider: gradle
Implementation-Url: http://incubator.apache.org/projects/s4.html
Implementation-Version: 0.5.0-SNAPSHOT
Implementation-Vendor: Apache S4
Implementation-Vendor-Id: io.s4
S4-App-Class: HelloApp
</pre>

We will use the attribute prefix "S4-App-" for all S3-related properties.

* Files:
    HelloApp.class
    HelloApp.java
    MANIFEST.MF

* Create the jar file:
    jar cmf MANIFEST.MF HelloApp.jar HelloApp.class 

* Check:
    jar tvf HelloApp.jar 
      0 Mon Oct 17 16:31:18 PDT 2011 META-INF/
    275 Mon Oct 17 16:31:18 PDT 2011 META-INF/MANIFEST.MF
    702 Mon Oct 17 13:36:50 PDT 2011 HelloApp.class

