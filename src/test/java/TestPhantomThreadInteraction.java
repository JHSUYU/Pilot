import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.pilot.PilotUtil;
import org.pilot.concurrency.LockWrapper;
import org.pilot.concurrency.ThreadManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ThreadManager.class)
public class TestPhantomThreadInteraction {

    private LockWrapper lockWrapper;
    private final int NON_PHANTOM_THREADS = 5;
    private final int PHANTOM_THREADS = 10;
    private final int OPERATIONS_PER_THREAD = 10;

    // 用于跟踪abort的次数
    private AtomicInteger abortCount = new AtomicInteger(0);

    @Before
    public void setUp() {
        lockWrapper = new LockWrapper(new ReentrantLock());

        // 使用PowerMockito来mock静态方法
        PowerMockito.mockStatic(ThreadManager.class);

        // 设置ThreadManager.cleanupPhantomThreads的行为
        PowerMockito.doAnswer(invocation -> {
            String contextKey = invocation.getArgument(0);
            // 增加abort计数
            abortCount.incrementAndGet();
            System.out.println("Phantom thread aborted with context key: " + contextKey);
            return null;
        }).when(ThreadManager.class);
        ThreadManager.cleanupPhantomThreads(anyString());
    }

    @After
    public void tearDown() {
        lockWrapper = null;
        abortCount.set(0);
    }

    /**
     * 测试phantom thread和non-phantom thread交互
     * Phantom thread在检测到冲突时应该会自己abort
     */
    @Test
    public void testPhantomThreadAbortOnConflict() throws Exception {
        // 在这个测试中，我们使用CountDownLatch来控制线程执行顺序
        CountDownLatch readyLatch = new CountDownLatch(NON_PHANTOM_THREADS + PHANTOM_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch nonPhantomFinishLatch = new CountDownLatch(NON_PHANTOM_THREADS);
        CountDownLatch phantomFinishLatch = new CountDownLatch(PHANTOM_THREADS);

        // 记录成功获取锁的计数
        AtomicInteger nonPhantomLockCount = new AtomicInteger(0);
        AtomicInteger phantomLockCount = new AtomicInteger(0);

        // 创建并启动非phantom线程
        List<Thread> nonPhantomThreads = new ArrayList<>();
        for (int i = 0; i < NON_PHANTOM_THREADS; i++) {
            Thread t = new Thread(() -> {
                try {
                    // 准备就绪
                    readyLatch.countDown();
                    // 等待统一开始信号
                    startLatch.await();

                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        // 非phantom线程使用正常模式
                        try {
                            lockWrapper.lock();
                            try {
                                // 模拟工作
                                nonPhantomLockCount.incrementAndGet();
                                Thread.sleep(5); // 短暂持有锁
                            } finally {
                                lockWrapper.unlock();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail("Non-phantom thread should not fail: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Non-phantom thread failed: " + e.getMessage());
                } finally {
                    nonPhantomFinishLatch.countDown();
                }
            });
            nonPhantomThreads.add(t);
            t.start();
        }

        // 创建并启动phantom线程
        List<Thread> phantomThreads = new ArrayList<>();
        for (int i = 0; i < PHANTOM_THREADS; i++) {
            Thread t = new Thread(() -> {
                try (Scope scope = PilotUtil.getDryRunTraceScope(true)) {
                    // 准备就绪
                    readyLatch.countDown();
                    // 等待统一开始信号
                    startLatch.await();

                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        try {
                            // phantom线程使用干运行模式
                            lockWrapper.lock();
                            try {
                                // 模拟工作
                                phantomLockCount.incrementAndGet();
                                Thread.sleep(5); // 短暂持有锁
                            } finally {
                                lockWrapper.unlock();
                            }
                        } catch (Exception e) {
                            // 这里不算失败，因为phantom线程可能会被abort
                            System.out.println("Phantom thread operation interrupted: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Phantom thread failed: " + e.getMessage());
                } finally {
                    phantomFinishLatch.countDown();
                }
            });
            phantomThreads.add(t);
            t.start();
        }

        // 确保所有线程都准备就绪
        readyLatch.await();

        // 开始测试
        startLatch.countDown();

        // 等待所有线程完成
        boolean nonPhantomCompleted = nonPhantomFinishLatch.await(30, TimeUnit.SECONDS);
        //boolean phantomCompleted = phantomFinishLatch.await(30, TimeUnit.SECONDS);

        assertTrue("All non-phantom threads should complete", nonPhantomCompleted);
        //assertTrue("All phantom threads should complete or be aborted", phantomCompleted);

        System.out.println("Non-phantom successful lock acquisitions: " + nonPhantomLockCount.get());
        System.out.println("Phantom successful lock acquisitions: " + phantomLockCount.get());
        System.out.println("Phantom thread abort count: " + abortCount.get());

        // 确认非phantom线程成功执行了所有操作
        assertEquals("Non-phantom threads should acquire lock for all operations",
                NON_PHANTOM_THREADS * OPERATIONS_PER_THREAD,
                nonPhantomLockCount.get());

        // 验证cleanupPhantomThreads被调用
        PowerMockito.verifyStatic(ThreadManager.class, atLeastOnce());
        //ThreadManager.cleanupPhantomThreads(UUID.fromString(anyString()));

        // 我们期望在冲突时phantom线程会被abort，因此phantom线程获取锁的次数可能小于总操作数
        assertTrue("Phantom threads should have been aborted at least once",
                abortCount.get() > 0);
    }

    @Test
    public void testPhantomThreadsInteraction() throws Exception {
        CountDownLatch readyLatch = new CountDownLatch(PHANTOM_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(PHANTOM_THREADS);

        AtomicInteger lockCount = new AtomicInteger(0);

        List<Thread> phantomThreads = new ArrayList<>();
        for (int i = 0; i < PHANTOM_THREADS; i++) {
            Thread t = new Thread(() -> {
                try (Scope scope = PilotUtil.getDryRunTraceScope(true)) {
                    // 准备就绪
                    readyLatch.countDown();
                    // 等待统一开始信号
                    startLatch.await();

                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        try {
                            lockWrapper.lock();
                            try {
                                // 模拟工作
                                lockCount.incrementAndGet();
                                Thread.sleep(5); // 短暂持有锁
                            } finally {
                                lockWrapper.unlock();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail("Phantom thread operation failed: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Phantom thread failed: " + e.getMessage());
                } finally {
                    finishLatch.countDown();
                }
            });
            phantomThreads.add(t);
            t.start();
        }

        // 确保所有线程都准备就绪
        readyLatch.await();

        // 开始测试
        startLatch.countDown();

        // 等待所有线程完成
        boolean completed = finishLatch.await(30, TimeUnit.SECONDS);

        assertTrue("All phantom threads should complete", completed);

        System.out.println("Phantom successful lock acquisitions: " + lockCount.get());
        System.out.println("Phantom thread abort count: " + abortCount.get());

        // 确认所有phantom线程成功执行了所有操作
        assertEquals("Phantom threads should acquire lock for all operations",
                PHANTOM_THREADS * OPERATIONS_PER_THREAD,
                lockCount.get());

        // 验证cleanupPhantomThreads未被调用
        PowerMockito.verifyStatic(ThreadManager.class, never());
        ThreadManager.cleanupPhantomThreads(anyString());

        // 确认没有发生abort
        assertEquals("No phantom thread should be aborted", 0, abortCount.get());
    }


}