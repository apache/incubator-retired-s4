import java.net.URL;


public class Test {

    /**
     * @param args
     */
    public static void main(String[] args) {

        /* Get the URL for a resource. */
        Object o = new Object();
        URL url = o.getClass().getResource("/apps/HelloApp.jar");
        System.out.println("URL: " + url.toString());
        System.out.println("Filename: " + url.getFile().toString());
    }
}
