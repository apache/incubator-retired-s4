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
