package org.pilot.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.pilot.Constants.connectionString;
import static org.pilot.PilotUtil.isDryRun;

public class WrappedZookeeper extends ZooKeeper {
    private static final Logger LOG = LoggerFactory.getLogger(WrappedZookeeper.class);
    private static final String PILOT_PREFIX = "/pilot-zk";
    private ZooKeeper delegate;
    private final AtomicBoolean pilotInitialized = new AtomicBoolean(false);

    public WrappedZookeeper(ZooKeeper delegate, String cnString) throws IOException {
        // WrappedZookeeper itself also connects to ZooKeeper
        super(cnString, 30000, null, false);
        this.delegate = delegate;
    }

    private void initializePilotStructure() throws KeeperException, InterruptedException {
        // Only initialize once
        if (!isDryRun() || pilotInitialized.get()) {
            return;
        }

        synchronized (pilotInitialized) {
            if (pilotInitialized.get()) {
                return;
            }

            // If /pilot already exists, mark as initialized and return
            if (super.exists(PILOT_PREFIX, false) != null) {
                LOG.info("/pilot already exists, skipping initialization");
                pilotInitialized.set(true);
                return;
            }

            // Create /pilot root if it doesn't exist
            LOG.info("Creating /pilot root");
            super.create(PILOT_PREFIX, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // Recursively copy the entire tree from / to /pilot using delegate
            copyTree("/", PILOT_PREFIX);

            pilotInitialized.set(true);
            LOG.info("Pilot structure initialization completed");
        }
    }

    private void copyTree(String sourcePath, String destPath) throws KeeperException, InterruptedException {
        // Skip if source is already under /pilot to avoid infinite recursion
        if (sourcePath.startsWith(PILOT_PREFIX)) {
            return;
        }

        try {
            // Get data and stat from source using delegate
            Stat sourceStat = new Stat();
            byte[] data = delegate.getData(sourcePath, false, sourceStat);

            // Create corresponding path under /pilot using this (WrappedZookeeper's own connection)
            String targetPath = destPath + (sourcePath.equals("/") ? "" : sourcePath);
            if (!targetPath.equals(PILOT_PREFIX) && super.exists(targetPath, false) == null) {
                // Get ACL from source using delegate
                List<ACL> acl = delegate.getACL(sourcePath, new Stat());

                // Determine create mode based on ephemeral owner
                CreateMode mode = sourceStat.getEphemeralOwner() != 0 ?
                        CreateMode.EPHEMERAL : CreateMode.PERSISTENT;

                // Use super.create to create in pilot area
                super.create(targetPath, data, acl, mode);
                LOG.debug("Copied node: {} to {}", sourcePath, targetPath);
            }

            // Get children from delegate and recursively copy
            List<String> children = delegate.getChildren(sourcePath, false);
            for (String child : children) {
                String childSourcePath = sourcePath.equals("/") ? "/" + child : sourcePath + "/" + child;
                copyTree(childSourcePath, destPath);
            }
        } catch (KeeperException.NoNodeException e) {
            // Node doesn't exist, skip
            LOG.debug("Node {} doesn't exist, skipping", sourcePath);
        }
    }

    private String redirectPath(String path) {
        if (path == null) {
            return null;
        }
        if (isDryRun()) {
            // If already under /pilot, return as is
            if (path.startsWith(PILOT_PREFIX)) {
                return path;
            }
            // Redirect to /pilot prefix
            return path.equals("/") ? PILOT_PREFIX : PILOT_PREFIX + path;
        }
        return path;
    }

    // Override all read/write operations

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            String redirectedPath = redirectPath(path);
            String result = super.create(redirectedPath, data, acl, createMode);
            // If we redirected, strip the /pilot prefix from the result
            if (result != null && result.startsWith(PILOT_PREFIX)) {
                return result.substring(PILOT_PREFIX.length());
            }
            return result;
        } else {
            return delegate.create(path, data, acl, createMode);
        }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            String redirectedPath = redirectPath(path);
            String result = super.create(redirectedPath, data, acl, createMode, stat);
            if (result != null && result.startsWith(PILOT_PREFIX)) {
                return result.substring(PILOT_PREFIX.length());
            }
            return result;
        } else {
            return delegate.create(path, data, acl, createMode, stat);
        }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat, long ttl)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            String redirectedPath = redirectPath(path);
            String result = super.create(redirectedPath, data, acl, createMode, stat, ttl);
            if (result != null && result.startsWith(PILOT_PREFIX)) {
                return result.substring(PILOT_PREFIX.length());
            }
            return result;
        } else {
            return delegate.create(path, data, acl, createMode, stat, ttl);
        }
    }

    @Override
    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode,
                       AsyncCallback.StringCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            String redirectedPath = redirectPath(path);
            // Wrap callback to strip /pilot prefix from result
            AsyncCallback.StringCallback wrappedCb = (rc, p, c, name) -> {
                String resultName = name;
                if (name != null && name.startsWith(PILOT_PREFIX)) {
                    resultName = name.substring(PILOT_PREFIX.length());
                }
                cb.processResult(rc, path, c, resultName);
            };
            super.create(redirectedPath, data, acl, createMode, wrappedCb, ctx);
        } else {
            delegate.create(path, data, acl, createMode, cb, ctx);
        }
    }

    @Override
    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode,
                       AsyncCallback.Create2Callback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            String redirectedPath = redirectPath(path);
            AsyncCallback.Create2Callback wrappedCb = (rc, p, c, name, stat) -> {
                String resultName = name;
                if (name != null && name.startsWith(PILOT_PREFIX)) {
                    resultName = name.substring(PILOT_PREFIX.length());
                }
                cb.processResult(rc, path, c, resultName, stat);
            };
            super.create(redirectedPath, data, acl, createMode, wrappedCb, ctx);
        } else {
            delegate.create(path, data, acl, createMode, cb, ctx);
        }
    }

    @Override
    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode,
                       AsyncCallback.Create2Callback cb, Object ctx, long ttl) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            String redirectedPath = redirectPath(path);
            AsyncCallback.Create2Callback wrappedCb = (rc, p, c, name, stat) -> {
                String resultName = name;
                if (name != null && name.startsWith(PILOT_PREFIX)) {
                    resultName = name.substring(PILOT_PREFIX.length());
                }
                cb.processResult(rc, path, c, resultName, stat);
            };
            super.create(redirectedPath, data, acl, createMode, wrappedCb, ctx, ttl);
        } else {
            delegate.create(path, data, acl, createMode, cb, ctx, ttl);
        }
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        initializePilotStructure();

        if (isDryRun()) {
            super.delete(redirectPath(path), version);
        } else {
            delegate.delete(path, version);
        }
    }

    @Override
    public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx);
            return;
        }

        if (isDryRun()) {
            super.delete(redirectPath(path), version, (rc, p, c) -> cb.processResult(rc, path, c), ctx);
        } else {
            delegate.delete(path, version, cb, ctx);
        }
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.exists(redirectPath(path), watcher);
        } else {
            return delegate.exists(path, watcher);
        }
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.exists(redirectPath(path), watch);
        } else {
            return delegate.exists(path, watch);
        }
    }

    @Override
    public void exists(String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            super.exists(redirectPath(path), watcher, (rc, p, c, stat) -> cb.processResult(rc, path, c, stat), ctx);
        } else {
            delegate.exists(path, watcher, cb, ctx);
        }
    }

    @Override
    public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            super.exists(redirectPath(path), watch, (rc, p, c, stat) -> cb.processResult(rc, path, c, stat), ctx);
        } else {
            delegate.exists(path, watch, cb, ctx);
        }
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getData(redirectPath(path), watcher, stat);
        } else {
            return delegate.getData(path, watcher, stat);
        }
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getData(redirectPath(path), watch, stat);
        } else {
            return delegate.getData(path, watch, stat);
        }
    }

    @Override
    public void getData(String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            super.getData(redirectPath(path), watcher,
                    (rc, p, c, data, stat) -> cb.processResult(rc, path, c, data, stat), ctx);
        } else {
            delegate.getData(path, watcher, cb, ctx);
        }
    }

    @Override
    public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            super.getData(redirectPath(path), watch,
                    (rc, p, c, data, stat) -> cb.processResult(rc, path, c, data, stat), ctx);
        } else {
            delegate.getData(path, watch, cb, ctx);
        }
    }

    @Override
    public Stat setData(String path, byte[] data, int version)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.setData(redirectPath(path), data, version);
        } else {
            return delegate.setData(path, data, version);
        }
    }

    @Override
    public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            super.setData(redirectPath(path), data, version,
                    (rc, p, c, stat) -> cb.processResult(rc, path, c, stat), ctx);
        } else {
            delegate.setData(path, data, version, cb, ctx);
        }
    }

    @Override
    public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getACL(redirectPath(path), stat);
        } else {
            return delegate.getACL(path, stat);
        }
    }

    @Override
    public void getACL(String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            super.getACL(redirectPath(path), stat,
                    (rc, p, c, acl, s) -> cb.processResult(rc, path, c, acl, s), ctx);
        } else {
            delegate.getACL(path, stat, cb, ctx);
        }
    }

    @Override
    public Stat setACL(String path, List<ACL> acl, int aclVersion)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.setACL(redirectPath(path), acl, aclVersion);
        } else {
            return delegate.setACL(path, acl, aclVersion);
        }
    }

    @Override
    public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            super.setACL(redirectPath(path), acl, version,
                    (rc, p, c, stat) -> cb.processResult(rc, path, c, stat), ctx);
        } else {
            delegate.setACL(path, acl, version, cb, ctx);
        }
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getChildren(redirectPath(path), watcher);
        } else {
            return delegate.getChildren(path, watcher);
        }
    }

    @Override
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getChildren(redirectPath(path), watch);
        } else {
            return delegate.getChildren(path, watch);
        }
    }

    @Override
    public void getChildren(String path, Watcher watcher, AsyncCallback.ChildrenCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            super.getChildren(redirectPath(path), watcher,
                    (rc, p, c, children) -> cb.processResult(rc, path, c, children), ctx);
        } else {
            delegate.getChildren(path, watcher, cb, ctx);
        }
    }

    @Override
    public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null);
            return;
        }

        if (isDryRun()) {
            super.getChildren(redirectPath(path), watch,
                    (rc, p, c, children) -> cb.processResult(rc, path, c, children), ctx);
        } else {
            delegate.getChildren(path, watch, cb, ctx);
        }
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher, Stat stat)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getChildren(redirectPath(path), watcher, stat);
        } else {
            return delegate.getChildren(path, watcher, stat);
        }
    }

    @Override
    public List<String> getChildren(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        initializePilotStructure();

        if (isDryRun()) {
            return super.getChildren(redirectPath(path), watch, stat);
        } else {
            return delegate.getChildren(path, watch, stat);
        }
    }

    @Override
    public void getChildren(String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            super.getChildren(redirectPath(path), watcher,
                    (rc, p, c, children, stat) -> cb.processResult(rc, path, c, children, stat), ctx);
        } else {
            delegate.getChildren(path, watcher, cb, ctx);
        }
    }

    @Override
    public void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx, null, null);
            return;
        }

        if (isDryRun()) {
            super.getChildren(redirectPath(path), watch,
                    (rc, p, c, children, stat) -> cb.processResult(rc, path, c, children, stat), ctx);
        } else {
            delegate.getChildren(path, watch, cb, ctx);
        }
    }

    @Override
    public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), path, ctx);
            return;
        }

        if (isDryRun()) {
            super.sync(redirectPath(path), (rc, p, c) -> cb.processResult(rc, path, c), ctx);
        } else {
            delegate.sync(path, cb, ctx);
        }
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
        initializePilotStructure();

        if (isDryRun()) {
            List<Op> redirectedOps = new ArrayList<>();
            for (Op op : ops) {
                redirectedOps.add(redirectOp(op));
            }
            return super.multi(redirectedOps);
        } else {
            return delegate.multi(ops);
        }
    }

    @Override
    public void multi(Iterable<Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        try {
            initializePilotStructure();
        } catch (Exception e) {
            cb.processResult(KeeperException.Code.SYSTEMERROR.intValue(), null, ctx, null);
            return;
        }

        if (isDryRun()) {
            List<Op> redirectedOps = new ArrayList<>();
            for (Op op : ops) {
                redirectedOps.add(redirectOp(op));
            }
            super.multi(redirectedOps, cb, ctx);
        } else {
            delegate.multi(ops, cb, ctx);
        }
    }

    private Op redirectOp(Op op) {
        try {
            // Get the Op class (parent class)
            Class<?> opClass = Op.class;

            // Get the private 'path' field
            Field pathField = opClass.getDeclaredField("path");
            pathField.setAccessible(true);

            // Get the current path value using reflection
            String path = (String) pathField.get(op);

            if (path == null) {
                return op;
            }

            String redirectedPath = redirectPath(path);
            if (redirectedPath.equals(path)) {
                return op;
            }

            // Create a new Op instance based on the type
            Op newOp = null;

            if (op instanceof Op.Create) {
                Op.Create createOp = (Op.Create) op;
                // Access the protected fields using reflection
                Field dataField = Op.Create.class.getDeclaredField("data");
                Field aclField = Op.Create.class.getDeclaredField("acl");
                Field flagsField = Op.Create.class.getDeclaredField("flags");
                dataField.setAccessible(true);
                aclField.setAccessible(true);
                flagsField.setAccessible(true);

                byte[] data = (byte[]) dataField.get(createOp);
                @SuppressWarnings("unchecked")
                List<ACL> acl = (List<ACL>) aclField.get(createOp);
                int flags = flagsField.getInt(createOp);

                newOp = Op.create(redirectedPath, data, acl, flags);
            } else if (op instanceof Op.CreateTTL) {
                Op.CreateTTL createTTLOp = (Op.CreateTTL) op;
                // Access fields from parent class Create
                Field dataField = Op.Create.class.getDeclaredField("data");
                Field aclField = Op.Create.class.getDeclaredField("acl");
                Field flagsField = Op.Create.class.getDeclaredField("flags");
                Field ttlField = Op.CreateTTL.class.getDeclaredField("ttl");
                dataField.setAccessible(true);
                aclField.setAccessible(true);
                flagsField.setAccessible(true);
                ttlField.setAccessible(true);

                byte[] data = (byte[]) dataField.get(createTTLOp);
                @SuppressWarnings("unchecked")
                List<ACL> acl = (List<ACL>) aclField.get(createTTLOp);
                int flags = flagsField.getInt(createTTLOp);
                long ttl = ttlField.getLong(createTTLOp);

                newOp = Op.create(redirectedPath, data, acl, flags, ttl);
            } else if (op instanceof Op.Delete) {
                Op.Delete deleteOp = (Op.Delete) op;
                Field versionField = Op.Delete.class.getDeclaredField("version");
                versionField.setAccessible(true);
                int version = versionField.getInt(deleteOp);
                newOp = Op.delete(redirectedPath, version);
            } else if (op instanceof Op.SetData) {
                Op.SetData setDataOp = (Op.SetData) op;
                Field dataField = Op.SetData.class.getDeclaredField("data");
                Field versionField = Op.SetData.class.getDeclaredField("version");
                dataField.setAccessible(true);
                versionField.setAccessible(true);
                byte[] data = (byte[]) dataField.get(setDataOp);
                int version = versionField.getInt(setDataOp);
                newOp = Op.setData(redirectedPath, data, version);
            } else if (op instanceof Op.Check) {
                Op.Check checkOp = (Op.Check) op;
                Field versionField = Op.Check.class.getDeclaredField("version");
                versionField.setAccessible(true);
                int version = versionField.getInt(checkOp);
                newOp = Op.check(redirectedPath, version);
            } else {
                // For any unknown Op types, try to directly modify the path field
                pathField.set(op, redirectedPath);
                return op;
            }

            return newOp;

        } catch (NoSuchFieldException | IllegalAccessException e) {
            // If reflection fails, log error and return original op
            LOG.error("Failed to redirect Op", e);
            return op;
        }
    }

    // Delegate other methods based on isDryRun()

    @Override
    public long getSessionId() {
        if (isDryRun()) {
            return super.getSessionId();
        } else {
            return delegate.getSessionId();
        }
    }

    @Override
    public byte[] getSessionPasswd() {
        if (isDryRun()) {
            return super.getSessionPasswd();
        } else {
            return delegate.getSessionPasswd();
        }
    }

    @Override
    public int getSessionTimeout() {
        if (isDryRun()) {
            return super.getSessionTimeout();
        } else {
            return delegate.getSessionTimeout();
        }
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        if (isDryRun()) {
            super.addAuthInfo(scheme, auth);
        } else {
            delegate.addAuthInfo(scheme, auth);
        }
    }

    @Override
    public synchronized void register(Watcher watcher) {
        if (isDryRun()) {
            super.register(watcher);
        } else {
            delegate.register(watcher);
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
        try {
            if (isDryRun()) {
                super.close();
            }
        } finally {
            // Always close delegate connection
            if (delegate != null) {
                delegate.close();
            }
        }
    }

    @Override
    public States getState() {
        if (isDryRun()) {
            return super.getState();
        } else {
            return delegate.getState();
        }
    }
}