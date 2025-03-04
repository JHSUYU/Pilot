import org.pilot.concurrency.ThreadManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pilot.zookeeper.ZooKeeperClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for ThreadManager.
 * Requires a running ZooKeeper instance.
 * Set the ZooKeeper connection string in the ZOOKEEPER_CONNECTION_STRING constant.
 */
public class ThreadManagerIntegrationTest {

    // Configure your ZooKeeper connection string here
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";

    private ZooKeeperClient zkClient;
    private final UUID mockUUID = UUID.fromString(ThreadManager.mockID);
    private final String nodePath = ThreadManager.DRY_RUN_PATH + "/" + mockUUID.toString();

    @BeforeEach
    public void setUp() throws Exception {
        // Connect to ZooKeeper directly to set up test environment
        zkClient = new ZooKeeperClient(ZOOKEEPER_CONNECTION_STRING);
        zkClient.connect();

        // Ensure the dry run path exists
        if (!zkClient.exists(ThreadManager.DRY_RUN_PATH)) {
            zkClient.createRecursive(ThreadManager.DRY_RUN_PATH);
        }

        // Clean up any existing test node
        if (zkClient.exists(nodePath)) {
            zkClient.delete(nodePath);
        }

        // Reset the static map
        ThreadManager.phantomThreadMap = new ConcurrentHashMap<>();
        // Add the mock UUID entry with an empty list
        ThreadManager.phantomThreadMap.put(mockUUID, new ArrayList<>());
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Clean up after tests
        if (zkClient.exists(nodePath)) {
            zkClient.delete(nodePath);
        }

        // Close ZooKeeper connection
        zkClient.close();
    }

    @Test
    public void testRegisterAndUnregisterNode() throws Exception {
        // Register the node
        ThreadManager.registerNode();

        // Verify the node exists
        assertTrue(zkClient.exists(nodePath), "Node should exist after registration");

        // Unregister the node
        ThreadManager.unregisterNode();

        // Verify the node no longer exists
        assertFalse(zkClient.exists(nodePath), "Node should not exist after unregistration");
    }

    @Test
    public void testTrackDryRunThread() {
        // Create a thread to track
        Thread testThread = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
        });

        // Track the thread
        Thread result = ThreadManager.trackDryRunThread(testThread);

        // Verify it was added to the map
        assertTrue(ThreadManager.phantomThreadMap.get(UUID.fromString(ThreadManager.mockID)).contains(testThread),
                "Thread should be added to the phantom thread map");
        assertEquals(testThread, result, "Tracked thread should be returned");
    }

    @Test
    public void testCleanupPhantomThreads() throws Exception {
        // Create threads to track
        CountDownLatch latch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            try {
                latch.countDown();
                Thread.sleep(300000); // Long sleep to ensure thread is still running during test
            } catch (InterruptedException ignored) {
                // Expected to be interrupted
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                latch.countDown();
                Thread.sleep(300000); // Long sleep to ensure thread is still running during test
            } catch (InterruptedException ignored) {
                // Expected to be interrupted
            }
        });

        // Start the threads
        t1.start();
        t2.start();

        // Wait for threads to start
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Threads should start within timeout");

        // Add them to the phantom map
        List<Thread> threads = new ArrayList<>();
        threads.add(t1);
        threads.add(t2);
        ThreadManager.phantomThreadMap.put(mockUUID, threads);

        // Call cleanup
        ThreadManager.cleanupPhantomThreads(mockUUID);

        // Give some time for interruption to take effect
        TimeUnit.MILLISECONDS.sleep(500);

        // Verify cleanup occurred
        assertFalse(ThreadManager.phantomThreadMap.containsKey(mockUUID),
                "UUID should be removed from phantom map");
        assertTrue(t1.isInterrupted(), "Thread 1 should be interrupted");
        assertTrue(t2.isInterrupted(), "Thread 2 should be interrupted");

        // Wait for threads to terminate
        t1.join(1000);
        t2.join(1000);
    }

    @Test
    public void testWatcherTriggersCleanup() throws Exception {
        // Create a watched thread
        CountDownLatch threadStarted = new CountDownLatch(1);
        CountDownLatch threadInterrupted = new CountDownLatch(1);

        Thread watchedThread = new Thread(() -> {
            try {
                threadStarted.countDown();
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                threadInterrupted.countDown();
            }
        });

        // Start the thread
        watchedThread.start();

        // Wait for thread to start
        assertTrue(threadStarted.await(5, TimeUnit.SECONDS), "Thread should start within timeout");

        // Add thread to phantom map
        List<Thread> threads = new ArrayList<>();
        threads.add(watchedThread);
        ThreadManager.phantomThreadMap.put(mockUUID, threads);

        // Register the node (which should create the ZK node)
        ThreadManager.registerNode();

        // Verify node exists
        assertTrue(zkClient.exists(nodePath), "Node should exist after registration");

        // Delete the node to trigger the watcher
        zkClient.delete(nodePath);

        // Wait for the thread to be interrupted (verifies the watcher worked)
        assertTrue(threadInterrupted.await(10, TimeUnit.SECONDS),
                "Thread should be interrupted when node is deleted");

        // Verify the thread was removed from phantom map
        assertFalse(ThreadManager.phantomThreadMap.containsKey(mockUUID),
                "UUID should be removed from phantom map after node deletion");

        // Wait for thread to terminate
        watchedThread.join(1000);
    }

    @Test
    public void testFullLifecycle() throws Exception {
        // 1. Create threads to track
        CountDownLatch threadsStarted = new CountDownLatch(3);
        CountDownLatch threadsInterrupted = new CountDownLatch(3);

        List<Thread> testThreads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Thread t = new Thread(() -> {
                try {
                    threadsStarted.countDown();
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    threadsInterrupted.countDown();
                }
            });
            testThreads.add(t);
            t.start();
        }

        // Wait for threads to start
        assertTrue(threadsStarted.await(5, TimeUnit.SECONDS), "Threads should start within timeout");

        // 2. Track the threads
        for (Thread t : testThreads) {
            ThreadManager.trackDryRunThread(t);
        }

        // 3. Register the node
        ThreadManager.registerNode();

        // Verify node exists
        assertTrue(zkClient.exists(nodePath), "Node should exist after registration");

        // 4. Unregister the node (this should trigger cleanup via watcher)
        ThreadManager.unregisterNode();

        // 5. Wait for the threads to be interrupted
        assertTrue(threadsInterrupted.await(10, TimeUnit.SECONDS),
                "All threads should be interrupted when node is deleted");

        // Verify all threads were interrupted
        try{
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore
        }
        // 6. Verify the phantom map entry was removed
        assertFalse(ThreadManager.phantomThreadMap.containsKey(mockUUID),
                "UUID should be removed from phantom map after node deletion");

        // Wait for all threads to terminate
        for (Thread t : testThreads) {
            t.join(1000);
            assertFalse(t.isAlive(), "Thread should be terminated");
        }
    }
}