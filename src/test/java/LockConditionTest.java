import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.pilot.PilotUtil;
import org.pilot.concurrency.ConditionVariableWrapper;
import org.pilot.concurrency.LockWrapper;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LockConditionTest {

    public static int res=0;

    static class BoundedBuffer {
        private final LockWrapper lock;
        private final Condition notFull, notEmpty;
        private final int[] buf;
        private int head, tail, count;

        BoundedBuffer(int capacity, boolean dryRun) {
            ReentrantLock real = new ReentrantLock();
            this.lock = new LockWrapper(real);
            this.notFull = new ConditionVariableWrapper(real.newCondition(), lock);
            this.notEmpty = new ConditionVariableWrapper(real.newCondition(), lock);
            this.buf = new int[capacity];
        }

        public void put(int x) throws InterruptedException {
            assert PilotUtil.isDryRun();
            lock.lock();
            try {
                while (count == buf.length) {
                    notFull.await();
                }
                buf[tail] = x;
                tail = (tail + 1) % buf.length;
                count++;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        public int take() throws InterruptedException {
            assert PilotUtil.isDryRun();
            lock.lock();
            try {
                while (count == 0) {
                    notEmpty.await();
                }
                int x = buf[head];
                head = (head + 1) % buf.length;
                count--;
                notFull.signal();
                return x;
            } finally {
                lock.unlock();
            }
        }
    }


    public static void main(String[] args) throws Exception {
//        System.out.println("=== TEST 1: NORMAL mode ===");
//        runTest(false);

        System.out.println("\n=== TEST 2: DRY-RUN mode ===");
        for(int i=0;i<1000;i++){
            runTest(true);
        }
        System.out.println("result: " + res);
    }

    private static void runTest(boolean dryRun) throws Exception {
        final BoundedBuffer buf = new BoundedBuffer(3, dryRun);
        Thread producer = new Thread(() -> {
            try (Scope scope = PilotUtil.getDryRunTraceScope(dryRun)) {
                assert PilotUtil.isDryRun();
                try {
                    for (int i = 1; i <= 10; i++) {
                        buf.put(i);
                        System.out.println("[P] put " + i);
                        Thread.sleep(50);
                    }
                } catch (InterruptedException e) {
                    System.out.println("[P] interrupted");
                }
            }
        });

        Thread consumer = new Thread(() -> {
            try (Scope scope = PilotUtil.getDryRunTraceScope(dryRun)){
                assert PilotUtil.isDryRun();
            try {
                for (int i = 1; i <= 10; i++) {
                    int v = buf.take();
                    System.out.println("    [C] took " + v);
                    Thread.sleep(80);
                }
            } catch (InterruptedException e) {
                System.out.println("[C] interrupted");
            }
        }});

        producer.start();
        consumer.start();

        producer.join();
        consumer.join();
        System.out.println(">>> Test complete (dryRun=" + dryRun + ")");
        res++;

    }

}
