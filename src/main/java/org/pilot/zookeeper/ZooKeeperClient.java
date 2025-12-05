package org.pilot.zookeeper;

import io.opentelemetry.context.Context;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pilot.concurrency.ThreadManager.DRY_RUN_PATH;

import org.apache.zookeeper.ZooKeeper;
import org.pilot.PilotUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.pilot.concurrency.ThreadManager.DRY_RUN_PATH;

public class ZooKeeperClient {
    public enum EventType {
        NODE_CREATED,
        NODE_DELETED,
        NODE_CHANGED
    }

    public org.apache.zookeeper.ZooKeeper zk;
    private String connectionString;
    private static final int SESSION_TIMEOUT = 50000;

    // 存储每个路径的当前子节点列表
    private Map<String, List<String>> pathChildrenMap = new ConcurrentHashMap<>();

    // 存储每个路径的观察者
    private Map<String, List<Watcher>> watcherMap = new ConcurrentHashMap<>();

    public interface Watcher {
        void process(WatchedEvent event);
    }

    public static class WatchedEvent {
        private final EventType type;
        private final String path;

        public WatchedEvent(EventType type, String path) {
            this.type = type;
            this.path = path;
        }

        public EventType getType() {
            return type;
        }

        public String getPath() {
            return path;
        }
    }

    private org.apache.zookeeper.Watcher connectionWatcher = new org.apache.zookeeper.Watcher() {
        @Override
        public void process(org.apache.zookeeper.WatchedEvent event) {
            if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("Connected to ZooKeeper");
            } else if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected) {
                System.out.println("Disconnected from ZooKeeper");
            }
        }
    };

    public ZooKeeperClient(String connectionString) {
        this.connectionString = connectionString;
    }

    public ZooKeeperClient(ZooKeeper zooKeeper){
        this.zk = zooKeeper;
    }

    public void connect() {
        try {
            zk = new ZooKeeper(
                    connectionString,
                    SESSION_TIMEOUT,
                    connectionWatcher
            );

            // Wait for connection to establish
            System.out.println("Initial ZooKeeper state: " + zk.getState());

            int retries = 15;
            while (zk.getState() != org.apache.zookeeper.ZooKeeper.States.CONNECTED && retries > 0) {
                Thread.sleep(500);
                retries--;
            }

            if (zk.getState() != org.apache.zookeeper.ZooKeeper.States.CONNECTED) {
                throw new RuntimeException("Failed to connect to ZooKeeper");
            }

            // Ensure the base path exists
            createRecursive(DRY_RUN_PATH);

        } catch (Exception e) {
            throw new RuntimeException("Error connecting to ZooKeeper", e);
        }
    }

    public void create(String path, byte[] data) {
        try {
            zk.create(
                    path,
                    data,  // 使用提供的数据
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    org.apache.zookeeper.CreateMode.PERSISTENT
            );
            System.out.println("Created node: " + path + " with data");
        } catch (org.apache.zookeeper.KeeperException.NodeExistsException e) {
            System.out.println("Node already exists: " + path);
        } catch (Exception e) {
            throw new RuntimeException("Error creating node: " + path, e);
        }
    }

    public void create(String path) {
        try {
            zk.create(
                    path,
                    new byte[0],  // Empty data
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    org.apache.zookeeper.CreateMode.PERSISTENT
            );
            System.out.println("Created node: " + path);
        } catch (org.apache.zookeeper.KeeperException.NodeExistsException e) {
            System.out.println("Node already exists: " + path);
        } catch (Exception e) {
            throw new RuntimeException("Error creating node: " + path, e);
        }
    }

    public void createRecursive(String path) {
        try {
            // Skip if root path or already exists
            if (path.equals("/") || exists(path)) {
                return;
            }

            // Create parent path first
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash > 0) {
                String parentPath = path.substring(0, lastSlash);
                createRecursive(parentPath);
            }

            // Create this path
            try {
                create(path);
            } catch (RuntimeException e) {
                // Ignore if node already exists (race condition)
                if (!exists(path)) {
                    throw e;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error creating path recursively: " + path, e);
        }
    }

    public boolean exists(String path) {
        try {
            return zk.exists(path, false) != null;
        } catch (Exception e) {
            throw new RuntimeException("Error checking node existence: " + path, e);
        }
    }

    public void delete(String path) {
        try {
            zk.delete(path, -1);  // -1 means any version
            System.out.println("Deleted node: " + path);
        } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
            System.out.println("Node does not exist: " + path);
        } catch (Exception e) {
            throw new RuntimeException("Error deleting node: " + path, e);
        }
    }

    /**
     * 监视指定路径的子节点变化
     *
     * @param path 要监视的路径
     * @param watcher 当子节点被删除时要通知的观察者
     */
    public void watchChildren(String path, Watcher watcher) {
        try {
            // 将观察者添加到观察者映射中
            watcherMap.computeIfAbsent(path, k -> new ArrayList<>()).add(watcher);

            // 获取初始子节点列表并设置监视器
            setupChildrenWatcher(path);

            System.out.println("Watching children of: " + path);
        } catch (Exception e) {
            throw new RuntimeException("Error setting up children watcher: " + path, e);
        }
    }

    /**
     * 设置子节点监视器并获取当前子节点列表
     */
    private void setupChildrenWatcher(String path) {
        try {

            List<String> currentChildren = zk.getChildren(path, event -> {

                if (event.getType() == org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        detectChildrenChanges(path);

                        setupChildrenWatcher(path);
                    } catch (Exception e) {
                        System.err.println("Error handling NodeChildrenChanged event: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            });

            pathChildrenMap.put(path, new ArrayList<>(currentChildren));

        } catch (Exception e) {
            System.err.println("Error setting up children watcher for path " + path + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 检测子节点的变化并通知相应的观察者
     */
    private void detectChildrenChanges(String path) {
        try {
            List<String> currentChildren = zk.getChildren(path, false);

            List<String> previousChildren = pathChildrenMap.getOrDefault(path, new ArrayList<>());

            List<String> deletedNodes = new ArrayList<>(previousChildren);
            deletedNodes.removeAll(currentChildren);

            List<String> addedNodes = new ArrayList<>(currentChildren);
            addedNodes.removeAll(previousChildren);

            for (String deletedNode : deletedNodes) {
                String deletedPath = path + "/" + deletedNode;
                System.out.println("Detected node deletion: " + deletedPath);

                List<Watcher> watchers = watcherMap.getOrDefault(path, new ArrayList<>());
                for (Watcher watcher : watchers) {
                    watcher.process(new WatchedEvent(EventType.NODE_DELETED, deletedPath));
                }
            }

            for (String addedNode : addedNodes) {
                String addedPath = path + "/" + addedNode;
                System.out.println("Detected node addition: " + addedPath);

                // 如果需要，可以添加类似的代码来通知观察者有关新增节点的信息
                // List<Watcher> watchers = watcherMap.getOrDefault(path, new ArrayList<>());
                // for (Watcher watcher : watchers) {
                //     watcher.process(new WatchedEvent(EventType.NODE_CREATED, addedPath));
                // }
            }

            // 更新存储的子节点列表
            pathChildrenMap.put(path, new ArrayList<>(currentChildren));

        } catch (Exception e) {
            System.err.println("Error detecting children changes for path " + path + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing ZooKeeper connection: " + e.getMessage());
        }
    }
}