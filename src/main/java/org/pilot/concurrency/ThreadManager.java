package org.pilot.concurrency;

import org.pilot.zookeeper.ZooKeeperClient;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

public class ThreadManager {

    public static Map<UUID, List<ScheduledFuture<?>>> phantomScheduledFutureMap = new ConcurrentHashMap<>();
    public static Map<UUID, List<Thread>> phantomThreadMap = new ConcurrentHashMap<>();

    private static ZooKeeperClient zooKeeperClient;

    public static final String DRY_RUN_PATH = "/dry-run";

    public static final String mockID = "00000000-0000-0000-0000-000000000000";

    public static final String connectionString = "node0:2181"; //


    static {
        initializeZooKeeper();
    }

    /**
     * Initialize ZooKeeper client and set up watchers.
     */
    private static void initializeZooKeeper() {
        try {
            zooKeeperClient = new ZooKeeperClient(connectionString);
            zooKeeperClient.connect();

            // Set up watcher for the dry-run directory in ZooKeeper
            zooKeeperClient.watchChildren(DRY_RUN_PATH, (event) -> {
                if (event.getType() == ZooKeeperClient.EventType.NODE_DELETED) {
                    String path = event.getPath();
                    String nodeName = path.substring(path.lastIndexOf('/') + 1);
                    System.out.println("Dry-run node deleted: " + nodeName);

                    try {
                        UUID uuid = UUID.fromString(nodeName);
                        cleanupPhantomThreads(uuid);
                        cleanupPhantomScheduledFutures(uuid);
                    } catch (IllegalArgumentException e) {
                        System.err.println("Invalid UUID format in deleted node: " + nodeName);
                    }
                }
            });

            System.out.println("Connected to ZooKeeper and watching path: " + DRY_RUN_PATH);
        } catch (Exception e) {
            System.err.println("Failed to initialize ZooKeeper: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void cleanupPhantomScheduledFutures(UUID dryRunId) {
        List<ScheduledFuture<?>> futures = phantomScheduledFutureMap.get(dryRunId);
        if (futures != null) {
            for (ScheduledFuture<?> future : futures) {
                if (!future.isDone()) {
                    System.out.println("Cancelling phantom scheduled future: " + future.toString());
                    future.cancel(true);
                }
            }
            phantomScheduledFutureMap.remove(dryRunId);
            System.out.println("Removed phantom scheduled futures for UUID: " + dryRunId.toString());
            System.out.println("Current phantom scheduled future map: " + phantomScheduledFutureMap.toString());
        }
    }

    public static void cleanupPhantomThreads(String dryRunId) {
        List<Thread> threads = phantomThreadMap.get(dryRunId);
        if (threads != null) {
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    System.out.println("Interrupting phantom thread: " + thread.getName() + " with ID: " + dryRunId.toString());
                    thread.interrupt();

                    // 等待一小段时间让中断状态传播
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // 忽略
                    }

                    System.out.println("Thread " + thread.getName() + " interrupted status: " + thread.isInterrupted());
                } else {
                    System.out.println("Thread " + thread.getName() + " is not alive, cannot interrupt");
                }
            }
            // 在interrupt之后才从map中移除
            phantomThreadMap.remove(dryRunId);
            System.out.println("Removed phantom threads for UUID: " + dryRunId.toString());
            System.out.println("Current phantom thread map: " + phantomThreadMap.toString());
        }
    }


    public static void cleanupPhantomThreads(UUID dryRunId) {
        List<Thread> threads = phantomThreadMap.get(dryRunId);
        if (threads != null) {
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    System.out.println("Interrupting phantom thread: " + thread.getName() + " with ID: " + dryRunId.toString());
                    thread.interrupt();

                    // 等待一小段时间让中断状态传播
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // 忽略
                    }

                    System.out.println("Thread " + thread.getName() + " interrupted status: " + thread.isInterrupted());
                } else {
                    System.out.println("Thread " + thread.getName() + " is not alive, cannot interrupt");
                }
            }
            // 在interrupt之后才从map中移除
            phantomThreadMap.remove(dryRunId);
            System.out.println("Removed phantom threads for UUID: " + dryRunId.toString());
            System.out.println("Current phantom thread map: " + phantomThreadMap.toString());
        }
    }

    public static Thread trackDryRunThread(Thread t) {
        // Add to phantom thread map
        phantomThreadMap.putIfAbsent(UUID.fromString(mockID), new java.util.ArrayList<>());
        phantomThreadMap.get(UUID.fromString(mockID)).add(t);
        return t;
    }

    public static void trackDryRunScheduledFuture(ScheduledFuture<?> future) {
        // Add to phantom thread map
        phantomScheduledFutureMap.putIfAbsent(UUID.fromString(mockID), new java.util.ArrayList<>());
        phantomScheduledFutureMap.get(UUID.fromString(mockID)).add(future);
    }



    public static void registerNode() {
        try {
            // Create a node with UUID "0" under the dry-run path
            UUID fixedUuid = UUID.fromString(mockID);
            String nodePath = DRY_RUN_PATH + "/" + fixedUuid.toString();

            // Check if the node already exists before creating
            if (!zooKeeperClient.exists(nodePath)) {
                zooKeeperClient.createRecursive(nodePath);
                System.out.println("Registered node: " + nodePath);
            } else {
                System.out.println("Node already exists: " + nodePath);
            }
        } catch (Exception e) {
            System.err.println("Failed to register node: " + e.getMessage());
        }
    }

    public static void unregisterNode() {
        try {
            // Delete the node with UUID "0" under the dry-run path
            UUID fixedUuid = UUID.fromString(mockID);
            String nodePath = DRY_RUN_PATH + "/" + fixedUuid.toString();

            // Check if the node exists before deleting
            if (zooKeeperClient.exists(nodePath)) {
                zooKeeperClient.delete(nodePath);
                System.out.println("Unregistered node: " + nodePath);
            } else {
                System.out.println("Node does not exist: " + nodePath);
            }
        } catch (Exception e) {
            System.err.println("Failed to unregister node: " + e.getMessage());
        }
    }

    public static boolean isPilotInProgress(String uuid){
        try {
            UUID fixedUuid = UUID.fromString(mockID);
            String nodePath = DRY_RUN_PATH + "/" + fixedUuid.toString();
            return zooKeeperClient.exists(nodePath);
        } catch (Exception e) {
            System.err.println("Failed to check node existence: " + e.getMessage());
            return false;
        }
    }


}
