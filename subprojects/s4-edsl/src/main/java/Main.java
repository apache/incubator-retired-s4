import org.apache.s4.core.edsl.BuilderS4DSL;

public class Main {
    public static BuilderS4DSL build() {
        return new BuilderS4DSL();
    }

    public static void main(String[] args) {

        String app = new BuilderS4DSL().pe().type().fireOn().ifInterval().cache().size().asSingleton().emitEvent()
                .onField().to().build();
        System.out.println(app);

    }
}
