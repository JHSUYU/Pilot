package org.pilot.filesystem;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Global lock manager for all Shadow FileSystem operations.
 * This ensures thread-safety across all file operations.
 */
public class GlobalLockManager {
    private static final ReentrantLock GLOBAL_LOCK = new ReentrantLock(true); // fair lock

    /**
     * Acquire the global lock
     */
    public static void lock() {
        GLOBAL_LOCK.lock();
    }

    /**
     * Release the global lock
     */
    public static void unlock() {
        GLOBAL_LOCK.unlock();
    }

    /**
     * Check if current thread holds the lock
     */
    public static boolean isHeldByCurrentThread() {
        return GLOBAL_LOCK.isHeldByCurrentThread();
    }

    /**
     * Execute a task with global lock
     */
    public static <T> T executeWithLock(LockableTask<T> task) throws Exception {
        lock();
        try {
            return task.execute();
        } finally {
            unlock();
        }
    }

    /**
     * Execute a task with global lock (for void operations)
     */
    public static void executeWithLock(LockableVoidTask task) throws Exception {
        lock();
        try {
            task.execute();
        } finally {
            unlock();
        }
    }

    @FunctionalInterface
    public interface LockableTask<T> {
        T execute() throws Exception;
    }

    @FunctionalInterface
    public interface LockableVoidTask {
        void execute() throws Exception;
    }
}