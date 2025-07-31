package org.pilot.concurrency;

import org.pilot.zookeeper.ZooKeeperClient;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import static org.pilot.PilotUtil.dryRunLog;
import static org.pilot.PilotUtil.executionIdGenerator;

public class ThreadManager {

    public static Map<UUID, List<ScheduledFuture<?>>> phantomScheduledFutureMap = new ConcurrentHashMap<>();
    public static Map<UUID, List<Thread>> phantomThreadMap = new ConcurrentHashMap<>();
    public static Map<String, List<Thread>> phantomWorkerThreadMap = new ConcurrentHashMap<>();

    public static Map<String, List<Thread>> phantomThreads = new ConcurrentHashMap<>();
    public static Map<String, List<ScheduledFuture<?>>> phantomScheduledFutures = new ConcurrentHashMap<>();

    private static ZooKeeperClient zooKeeperClient;

    public static final String DRY_RUN_PATH = "/dry-run";

    public static final String PILOT_PATH = "/pilot";

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
            zooKeeperClient.watchChildren(PILOT_PATH, (event) -> {
                if (event.getType() == ZooKeeperClient.EventType.NODE_DELETED) {
                    String path = event.getPath();
                    String nodeName = path.substring(path.lastIndexOf('/') + 1);
                    System.out.println("Pilot node deleted: " + nodeName);

                    try {
                        Long.parseLong(nodeName);

                        cleanupPhantomThreads(nodeName);
                        cleanupPhantomFutures(nodeName);

                    } catch (NumberFormatException e) {
                        System.err.println("Invalid numeric format in deleted node: " + nodeName);
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

    public static void cleanupPhantomFutures(String pilotID) {
        List<ScheduledFuture<?>> futures = phantomScheduledFutures.get(pilotID);
        if (futures != null) {
            for (ScheduledFuture<?> future : futures) {
                if (!future.isDone()) {
                    System.out.println("Cancelling phantom scheduled future: " + future.toString());
                    future.cancel(true);
                }
            }
        }
    }

    public static void cleanupPhantomThreads(String pilotId) {
        List<Thread> threads = phantomThreads.get(pilotId);
        if (threads != null) {
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    System.out.println("Interrupting phantom thread: " + thread.getName() + " with ID: " + pilotId);
                    thread.interrupt();

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }

                    System.out.println("Thread " + thread.getName() + " interrupted status: " + thread.isInterrupted());
                } else {
                    System.out.println("Thread " + thread.getName() + " is not alive, cannot interrupt");
                }
            }

            phantomThreads.remove(pilotId);
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

    public static Thread trackDryRunWorkerThread(Thread t) {
        // Add to phantom worker thread map
        phantomWorkerThreadMap.putIfAbsent(mockID, new java.util.ArrayList<>());
        phantomWorkerThreadMap.get(mockID).add(t);
        return t;
    }

    public static void startPhantomWorkerThread(){
        List<Thread> workerThreads = phantomWorkerThreadMap.get(mockID);
        if (workerThreads != null) {
            for (Thread workerThread : workerThreads) {
                if (workerThread.isAlive()) {
                    System.out.println("Starting phantom worker thread: " + workerThread.getName());
                    workerThread.start();
                } else {
                    System.out.println("Worker thread " + workerThread.getName() + " is not alive, cannot start");
                }
            }
        } else {
            System.out.println("No phantom worker threads to start.");
        }
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

    public static String generateExecutionIdFromZooKeeper() {
        try {
            ZooKeeperClient zkClient = getZooKeeperClient();

            if (zkClient == null || zkClient.zk == null ||
                    zkClient.zk.getState() != org.apache.zookeeper.ZooKeeper.States.CONNECTED) {
                dryRunLog("ZooKeeper client not connected, falling back to local ID generation");
                return null;
            }

            if (!zkClient.exists(ThreadManager.PILOT_PATH)) {
                zkClient.createRecursive(ThreadManager.PILOT_PATH);
                dryRunLog("Created PILOT_PATH: " + ThreadManager.PILOT_PATH);
            }

            List<String> existingNodes = zkClient.zk.getChildren(ThreadManager.PILOT_PATH, false);

            long maxId = 0;

            for (String node : existingNodes) {
                try {
                    long id = Long.parseLong(node);
                    maxId = Math.max(maxId, id);
                } catch (NumberFormatException e) {
                    dryRunLog("Ignoring non-numeric node: " + node);
                }
            }

            long newId = maxId + 1;

            int maxRetries = 10;
            for (int i = 0; i < maxRetries; i++) {
                String candidateId = String.valueOf(newId + i);
                String nodePath = ThreadManager.PILOT_PATH + "/" + candidateId;

                try {
                    if (!zkClient.exists(nodePath)) {
                        zkClient.create(nodePath);
                        dryRunLog("Successfully allocated execution ID: " + candidateId);
                        return candidateId;
                    }
                } catch (Exception e) {
                    dryRunLog("ID " + candidateId + " already exists, trying next...");
                }
            }

            String fallbackId = newId + "_" + System.currentTimeMillis();
            String fallbackPath = ThreadManager.PILOT_PATH + "/" + fallbackId;
            zkClient.create(fallbackPath);
            dryRunLog("Used fallback execution ID: " + fallbackId);
            return fallbackId;

        } catch (Exception e) {
            dryRunLog("Error generating execution ID from ZooKeeper: " + e.getMessage());
            e.printStackTrace();
            // 出错时降级到本地ID生成
            return String.valueOf(executionIdGenerator.incrementAndGet());
        }
    }

    public static ZooKeeperClient getZooKeeperClient() {
        return zooKeeperClient;
    }

    public static void cleanupExecutionId(String executionId) {
        try {
            ZooKeeperClient zkClient = getZooKeeperClient();
            if (zkClient != null) {
                String nodePath = ThreadManager.PILOT_PATH + "/" + executionId;
                if (zkClient.exists(nodePath)) {
                    zkClient.delete(nodePath);
                    dryRunLog("Cleaned up execution ID node: " + executionId);
                }
            }
        } catch (Exception e) {
            dryRunLog("Error cleaning up execution ID: " + e.getMessage());
        }
    }

    public static void registerPilotNodeWithId(String executionId) {
        try {
            ZooKeeperClient zkClient = getZooKeeperClient();
            if (zkClient != null) {
                String nodePath = ThreadManager.PILOT_PATH + "/" + executionId;

                if (!zkClient.exists(nodePath)) {
                    zkClient.create(nodePath);
                    dryRunLog("Registered pilot node with ID: " + executionId);
                } else {
                    dryRunLog("Pilot node already exists: " + executionId);
                }
            }
        } catch (Exception e) {
            dryRunLog("Failed to register pilot node: " + e.getMessage());
            e.printStackTrace();
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
