import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pilot.PilotUtil;
import org.pilot.concurrency.ConditionVariableWrapper;
import org.pilot.concurrency.LockWrapper;
import org.pilot.concurrency.ReentrantLockWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class TestReentrantLockWrapper {

    private ReentrantLockWrapper reentrantLockWrapper;
    private final int CONCURRENCY_LEVEL = 1000;
    private final int OPERATIONS_PER_THREAD = 10;
    public static int res = 0;

    // BoundedBuffer类定义，用于测试锁和条件变量的组合使用
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

    // ReentrantBoundedBuffer类，使用ReentrantLockWrapper
    static class ReentrantBoundedBuffer {
        private final ReentrantLockWrapper lock;
        private final Condition notFull, notEmpty;
        private final int[] buf;
        private int head, tail, count;

        ReentrantBoundedBuffer(int capacity) {
            ReentrantLock real = new ReentrantLock();
            this.lock = new ReentrantLockWrapper(real);
            this.notFull = real.newCondition();
            this.notEmpty = real.newCondition();
            this.buf = new int[capacity];
        }

        public void put(int x) throws InterruptedException {
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

    @Before
    public void setUp() {
        reentrantLockWrapper = new ReentrantLockWrapper(new ReentrantLock());
    }

    @After
    public void tearDown() {
        reentrantLockWrapper = null;
    }

    @Test
    public void testBasicLockAndUnlock() throws Exception {
        reentrantLockWrapper.lock();
        assertTrue("Lock should be held by current thread", reentrantLockWrapper.isHeldByCurrentThread());
        assertEquals("Hold count should be 1", 1, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        assertEquals("Hold count should be 0 after unlock", 0, reentrantLockWrapper.getHoldCount());
        assertFalse("Lock should not be held by current thread after unlock", reentrantLockWrapper.isHeldByCurrentThread());
    }

    @Test
    public void testReentrantProperty() throws Exception {
        // 测试可重入特性
        reentrantLockWrapper.lock();
        reentrantLockWrapper.lock();
        reentrantLockWrapper.lock();

        assertEquals("Hold count should be 3", 3, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        reentrantLockWrapper.unlock();
        reentrantLockWrapper.unlock();

        assertEquals("Hold count should be 0", 0, reentrantLockWrapper.getHoldCount());
    }

    @Test
    public void testTryLock() throws Exception {
        // 测试tryLock方法
        boolean acquired = reentrantLockWrapper.tryLock();
        assertTrue("Should acquire lock", acquired);
        assertTrue("Lock should be held by current thread", reentrantLockWrapper.isHeldByCurrentThread());

        // 测试tryLock的可重入性
        boolean reacquired = reentrantLockWrapper.tryLock();
        assertTrue("Should reacquire lock with tryLock", reacquired);
        assertEquals("Hold count should be 2", 2, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        reentrantLockWrapper.unlock();
    }

    @Test
    public void testTryLockWithTimeout() throws Exception {
        // 测试带超时的tryLock方法
        boolean acquired = reentrantLockWrapper.tryLock(1000, TimeUnit.MILLISECONDS);
        assertTrue("Should acquire lock with timeout", acquired);
        assertTrue("Lock should be held by current thread", reentrantLockWrapper.isHeldByCurrentThread());

        // 测试带超时的tryLock的可重入性
        boolean reacquired = reentrantLockWrapper.tryLock(1000, TimeUnit.MILLISECONDS);
        assertTrue("Should reacquire lock with tryLock timeout", reacquired);
        assertEquals("Hold count should be 2", 2, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        reentrantLockWrapper.unlock();
    }

    @Test
    public void testLockInterruptibly() throws Exception {
        // 测试可中断锁
        try {
            reentrantLockWrapper.lockInterruptibly();
            assertTrue("Lock should be held by current thread", reentrantLockWrapper.isHeldByCurrentThread());

            // 测试lockInterruptibly的可重入性
            reentrantLockWrapper.lockInterruptibly();
            assertEquals("Hold count should be 2", 2, reentrantLockWrapper.getHoldCount());

            reentrantLockWrapper.unlock();
            reentrantLockWrapper.unlock();
        } catch (InterruptedException e) {
            fail("Should not be interrupted");
        }
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlockByNonOwner() throws Exception {
        // 测试非持有线程解锁会抛出异常
        // 本测试中当前线程没有获取锁
        reentrantLockWrapper.unlock();
    }

    @Test
    public void testReentrantLockInterruptibly() throws Exception {
        // 测试lockInterruptibly的可中断性
        Thread t = new Thread(() -> {
            try {
                reentrantLockWrapper.lock();
                Thread.sleep(5000); // 持有锁5秒
            } catch (InterruptedException e) {
                reentrantLockWrapper.unlock();
            }
        });
        t.start();

        // 确保线程t获取到锁
        Thread.sleep(100);

        // 创建另一个线程尝试获取锁，但它会被中断
        final AtomicBoolean wasInterrupted = new AtomicBoolean(false);
        Thread t2 = new Thread(() -> {
            try {
                reentrantLockWrapper.lockInterruptibly();
            } catch (InterruptedException e) {
                wasInterrupted.set(true);
            }
        });
        t2.start();

        // 确保t2开始等待
        Thread.sleep(100);

        // 中断t2
        t2.interrupt();
        t2.join(1000);

        // 中断t1
        t.interrupt();
        t.join(1000);

        assertTrue("Thread should have been interrupted", wasInterrupted.get());
    }

    @Test
    public void testConcurrentLockOperations() throws Exception {
        // 测试高并发场景下的锁操作
        final int[] counter = {0};
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(CONCURRENCY_LEVEL);

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            Thread t = new Thread(() -> {
                try {
                    // 等待所有线程就绪
                    startLatch.await();

                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        reentrantLockWrapper.lock();
                        try {
                            counter[0]++;
                        } finally {
                            reentrantLockWrapper.unlock();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Thread operation failed: " + e.getMessage());
                } finally {
                    finishLatch.countDown();
                }
            });

            threads.add(t);
            t.start();
        }

        // 触发所有线程开始执行
        startLatch.countDown();

        // 等待所有线程完成
        finishLatch.await(60, TimeUnit.SECONDS);

        // 验证计数器值
        assertEquals("Counter should equal to expected operations count",
                CONCURRENCY_LEVEL * OPERATIONS_PER_THREAD,
                counter[0]);
    }

    @Test
    public void testConcurrentTryLockOperations() throws Exception {
        // 测试高并发场景下的tryLock操作
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failCount = new AtomicInteger(0);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(CONCURRENCY_LEVEL);

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            Thread t = new Thread(() -> {
                try {
                    // 等待所有线程就绪
                    startLatch.await();

                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        if (reentrantLockWrapper.tryLock()) {
                            try {
                                // 成功获取锁
                                successCount.incrementAndGet();
                                Thread.sleep(1); // 短暂持有锁
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                reentrantLockWrapper.unlock();
                            }
                        } else {
                            // 未获取到锁
                            failCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Thread operation failed: " + e.getMessage());
                } finally {
                    finishLatch.countDown();
                }
            });

            threads.add(t);
            t.start();
        }

        // 触发所有线程开始执行
        startLatch.countDown();

        // 等待所有线程完成
        finishLatch.await(60, TimeUnit.SECONDS);

        // 验证操作总数
        assertEquals("Total operations should match expected count",
                CONCURRENCY_LEVEL * OPERATIONS_PER_THREAD,
                successCount.get() + failCount.get());

        System.out.println("TryLock success count: " + successCount.get());
        System.out.println("TryLock failure count: " + failCount.get());
    }

    @Test
    public void testProducerConsumerWithReentrantLock() throws Exception {
        final ReentrantBoundedBuffer buf = new ReentrantBoundedBuffer(3);
        final AtomicInteger sum = new AtomicInteger(0);
        final int numItems = 100;
        final CountDownLatch finishLatch = new CountDownLatch(2);

        Thread producer = new Thread(() -> {
            try {
                for (int i = 1; i <= numItems; i++) {
                    buf.put(i);
                }
            } catch (InterruptedException e) {
                System.out.println("[P] interrupted");
            } finally {
                finishLatch.countDown();
            }
        });

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 1; i <= numItems; i++) {
                    int v = buf.take();
                    sum.addAndGet(v);
                }
            } catch (InterruptedException e) {
                System.out.println("[C] interrupted");
            } finally {
                finishLatch.countDown();
            }
        });

        producer.start();
        consumer.start();

        finishLatch.await(60, TimeUnit.SECONDS);

        int expectedSum = (numItems * (numItems + 1)) / 2; // 计算1到numItems的和
        assertEquals("Sum should equal to expected sum", expectedSum, sum.get());
    }

    @Test
    public void testDryRunMode() throws Exception {
        // 此测试依赖于PilotUtil.isDryRun()能够返回true
        // 在JUnit环境下可能需要额外设置，假设我们有一个方法可以做到这一点

        try (Scope scope = PilotUtil.getDryRunTraceScope(true)) {
            final BoundedBuffer buf = new BoundedBuffer(3, true);
            final CountDownLatch finishLatch = new CountDownLatch(2);
            final AtomicInteger sum = new AtomicInteger(0);
            final int numItems = 10;

            Thread producer = new Thread(() -> {
                try (Scope innerScope = PilotUtil.getDryRunTraceScope(true)) {
                    try {
                        for (int i = 1; i <= numItems; i++) {
                            buf.put(i);
                        }
                    } catch (InterruptedException e) {
                        System.out.println("[P] interrupted");
                    } finally {
                        finishLatch.countDown();
                    }
                }
            });

            Thread consumer = new Thread(() -> {
                try (Scope innerScope = PilotUtil.getDryRunTraceScope(true)) {
                    try {
                        for (int i = 1; i <= numItems; i++) {
                            int v = buf.take();
                            sum.addAndGet(v);
                        }
                    } catch (InterruptedException e) {
                        System.out.println("[C] interrupted");
                    } finally {
                        finishLatch.countDown();
                    }
                }
            });

            producer.start();
            consumer.start();

            finishLatch.await(60, TimeUnit.SECONDS);

            int expectedSum = (numItems * (numItems + 1)) / 2; // 计算1到numItems的和
            assertEquals("Sum should equal to expected sum in dry run mode", expectedSum, sum.get());
            res++; // 增加结果计数
        }
    }

    @Test
    public void testReentrantLockWithMixedLockTypes() throws Exception {
        // 测试同一线程使用不同方式获取锁的可重入性

        // 使用lock()获取锁
        reentrantLockWrapper.lock();
        assertEquals("Hold count should be 1", 1, reentrantLockWrapper.getHoldCount());

        // 使用tryLock()重入
        boolean reacquired = reentrantLockWrapper.tryLock();
        assertTrue("Should reacquire with tryLock", reacquired);
        assertEquals("Hold count should be 2", 2, reentrantLockWrapper.getHoldCount());

        // 使用tryLock(timeout)重入
        boolean timeoutReacquired = reentrantLockWrapper.tryLock(100, TimeUnit.MILLISECONDS);
        assertTrue("Should reacquire with tryLock(timeout)", timeoutReacquired);
        assertEquals("Hold count should be 3", 3, reentrantLockWrapper.getHoldCount());

        // 使用lockInterruptibly()重入
        reentrantLockWrapper.lockInterruptibly();
        assertEquals("Hold count should be 4", 4, reentrantLockWrapper.getHoldCount());

        // 依次解锁
        reentrantLockWrapper.unlock();
        assertEquals("Hold count should be 3", 3, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        assertEquals("Hold count should be 2", 2, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        assertEquals("Hold count should be 1", 1, reentrantLockWrapper.getHoldCount());

        reentrantLockWrapper.unlock();
        assertEquals("Hold count should be 0", 0, reentrantLockWrapper.getHoldCount());
    }

    @Test
    public void testMultipleDryRunCycles() throws Exception {
        // 多次运行dryRun模式，模拟多次Pilot运行
        for (int i = 0; i < 10000; i++) {
            try (Scope scope = PilotUtil.getDryRunTraceScope(true)) {
                final BoundedBuffer buf = new BoundedBuffer(3, true);
                final CountDownLatch finishLatch = new CountDownLatch(2);
                final AtomicInteger sum = new AtomicInteger(0);
                final int numItems = 5;

                Thread producer = new Thread(() -> {
                    try (Scope innerScope = PilotUtil.getDryRunTraceScope(true)) {
                        try {
                            for (int j = 1; j <= numItems; j++) {
                                buf.put(j);
                            }
                        } catch (InterruptedException e) {
                            System.out.println("[P] interrupted");
                        } finally {
                            finishLatch.countDown();
                        }
                    }
                });

                Thread consumer = new Thread(() -> {
                    try (Scope innerScope = PilotUtil.getDryRunTraceScope(true)) {
                        try {
                            for (int j = 1; j <= numItems; j++) {
                                int v = buf.take();
                                sum.addAndGet(v);
                            }
                        } catch (InterruptedException e) {
                            System.out.println("[C] interrupted");
                        } finally {
                            finishLatch.countDown();
                        }
                    }
                });

                producer.start();
                consumer.start();

                finishLatch.await(10, TimeUnit.SECONDS);

                int expectedSum = (numItems * (numItems + 1)) / 2;
                assertEquals("Sum should equal to expected sum in cycle " + i, expectedSum, sum.get());
                res++;
            }
        }
        System.out.println("Completed " + res + " dry run cycles successfully");
    }
}