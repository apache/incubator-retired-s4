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

## Create Counter Example JAR

Copy counter example classfiles:

<pre>
AgeKeyFinder.class         CountKeyFinder.class       GenderKeyFinder.class      Module.class               PrintPE.class              UserEvent.class            
CountEvent.class           CounterPE.class            GenerateUserEventPE.class  MyApp.class                README.md                  UserIDKeyFinder.class    
  
$ cat MANIFEST.MF
Manifest-Version: 1.0
provider: gradle
Implementation-Url: http://incubator.apache.org/projects/s4.html
Implementation-Version: 0.5.0-SNAPSHOT
Implementation-Vendor: Apache S4
Implementation-Vendor-Id: io.s4
S4-App-Class: io.s4.example.counter.MyApp
  
$ jar cmf MANIFEST.MF CounterExample.s4r org
$ jar tfv CounterExample.s4r 
     0 Sat Oct 22 19:21:06 PDT 2011 META-INF/
   307 Sat Oct 22 19:21:06 PDT 2011 META-INF/MANIFEST.MF
     0 Wed Oct 19 08:55:18 PDT 2011 org/
     0 Wed Oct 19 08:55:18 PDT 2011 org/apache/
     0 Wed Oct 19 08:55:36 PDT 2011 org/apache/s4/
     0 Wed Oct 19 08:55:44 PDT 2011 org/apache/s4/example/
     0 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/
  1240 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/AgeKeyFinder.class
  1708 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/CounterPE.class
  1330 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/CountEvent.class
  1197 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/CountKeyFinder.class
  1254 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/GenderKeyFinder.class
  2378 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/GenerateUserEventPE.class
  3414 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/Module.class
  4196 Sat Oct 22 19:19:14 PDT 2011 org/apache/s4/example/counter/MyApp.class
   897 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/PrintPE.class
   532 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/README.md
  1233 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/UserEvent.class
  1198 Wed Oct 19 08:55:18 PDT 2011 org/apache/s4/example/counter/UserIDKeyFinder.class

</pre>


