package org.pilot.concurrency;

import org.pilot.PilotUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ReentrantLockWrapper implements Lock {
    protected final Lock delegate;

    public AtomicBoolean delegateIsLocked = new AtomicBoolean(false);

    protected final AtomicLong ticketDispenser = new AtomicLong(0);
    protected final AtomicLong nextServeId = new AtomicLong(0);

    private Thread ownerThread = null;
    private int holdCount = 0;

    public ReentrantLockWrapper(Lock delegate) {
        this.delegate = delegate;
    }

    @Override
    public void lock() {
        Thread currentThread = Thread.currentThread();

        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            delegate.lock();
            if (ticketDispenser.get() > nextServeId.get()) {
                System.out.println("abort pilot execution");
                ThreadManager.cleanupPhantomThreads("");
            }
            ownerThread = currentThread;
            holdCount++;
            ticketDispenser.incrementAndGet();
        } else {
            if ((ticketDispenser.get() > nextServeId.get()) && currentThread == ownerThread && !delegateIsLocked.get()) {
                holdCount++;
                return;
            }

            long myTicket = ticketDispenser.getAndIncrement();
            while(nextServeId.get() != myTicket){
                Thread.yield();
            }
            while(delegateIsLocked.get()){
                Thread.yield();
            }
            ownerThread = currentThread;
            holdCount = 1;
        }
    }

    @Override
    public boolean tryLock() {
        Thread currentThread = Thread.currentThread();

        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            boolean acquired = delegate.tryLock();
            if (acquired) {
                if (ticketDispenser.get() > nextServeId.get()) {
                    System.out.println("abort pilot execution");
                    ThreadManager.cleanupPhantomThreads("");
                }
                ownerThread = currentThread;
                holdCount++;
                ticketDispenser.incrementAndGet();
            }
            return acquired;
        } else {
            if ((ticketDispenser.get() > nextServeId.get()) && currentThread == ownerThread && !delegateIsLocked.get()) {
                holdCount++;
                return true;
            }

            if (delegateIsLocked.get()) {
                return false;
            }

            long myTicket = ticketDispenser.getAndIncrement();
            if (nextServeId.get() == myTicket) {
                ownerThread = currentThread;
                holdCount = 1;
                return true;
            } else {
                ticketDispenser.decrementAndGet();
                return false;
            }
        }
    }

    @Override
    public void unlock() {
        holdCount--;

        if (!PilotUtil.isDryRun()) {
            delegate.unlock();
            if (holdCount == 0) {
                ownerThread = null;
                delegateIsLocked.set(false);
                ticketDispenser.decrementAndGet();
            }
        } else {
            if (holdCount == 0) {
                ownerThread = null;
                nextServeId.incrementAndGet();
            }
        }
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        Thread currentThread = Thread.currentThread();

        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            boolean acquired = delegate.tryLock(timeout, unit);
            if (acquired) {
                if (ticketDispenser.get() > nextServeId.get()) {
                    System.out.println("abort pilot execution");
                    ThreadManager.cleanupPhantomThreads("");
                }
                ownerThread = currentThread;
                holdCount++;
                ticketDispenser.incrementAndGet();
            }
            return acquired;
        } else {
            if ((ticketDispenser.get() > nextServeId.get()) && currentThread == ownerThread && !delegateIsLocked.get()) {
                holdCount++;
                return true;
            }

            if (delegateIsLocked.get()) {
                return false;
            }

            long myTicket = ticketDispenser.getAndIncrement();
            if (nextServeId.get() == myTicket) {
                ownerThread = currentThread;
                holdCount = 1;
                return true;
            }

            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (nextServeId.get() != myTicket) {
                if (System.nanoTime() > deadline) {
                    ticketDispenser.decrementAndGet();
                    return false;
                }
                Thread.yield();
            }

            while (delegateIsLocked.get()) {
                if (System.nanoTime() > deadline) {
                    ticketDispenser.decrementAndGet();
                    return false;
                }
                Thread.yield();
            }

            ownerThread = currentThread;
            holdCount = 1;
            return true;
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        Thread currentThread = Thread.currentThread();

        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            delegate.lockInterruptibly();
            if (ticketDispenser.get() > nextServeId.get()) {
                System.out.println("abort pilot execution");
                ThreadManager.cleanupPhantomThreads("");
            }
            ownerThread = currentThread;
            holdCount++;
            ticketDispenser.incrementAndGet();
        } else {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            if ((ticketDispenser.get() > nextServeId.get()) && currentThread == ownerThread && !delegateIsLocked.get()) {
                holdCount++;
                return;
            }

            long myTicket = ticketDispenser.getAndIncrement();
            while (nextServeId.get() != myTicket) {
                if (Thread.interrupted()) {
                    ticketDispenser.decrementAndGet();
                    throw new InterruptedException();
                }
                Thread.yield();
            }

            while (delegateIsLocked.get()) {
                if (Thread.interrupted()) {
                    ticketDispenser.decrementAndGet();
                    throw new InterruptedException();
                }
                Thread.yield();
            }

            ownerThread = currentThread;
            holdCount = 1;
        }
    }

    @Override
    public Condition newCondition() {
        return delegate.newCondition();
    }

    public boolean isHeldByCurrentThread() {
        return Thread.currentThread() == ownerThread;
    }

    public int getHoldCount() {
        return Thread.currentThread() == ownerThread ? holdCount : 0;
    }
}