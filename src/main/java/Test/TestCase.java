package Test;

import io.opentelemetry.context.Context;
import org.pilot.PilotUtil;

import static org.pilot.PilotUtil.getPilotContextInternal;

public class TestCase {

    public static void main(String[] args){
        TestThread t1 = new TestThread();
        Context c = Context.current();
        c=getPilotContextInternal(c, 1);
        c.makeCurrent();
        PilotUtil.start(t1);
        System.out.println(t1.PilotContext==null);
    }
}
