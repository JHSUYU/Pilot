package org.pilot.concurrency;

import io.opentelemetry.context.Context;
import org.pilot.PilotUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class LockWrapper implements Lock {
    protected final Lock delegate;
    public AtomicBoolean delegateIsLocked = new AtomicBoolean(false);

    protected final AtomicLong ticketDispenser = new AtomicLong(0);
    protected final AtomicLong nextServeId = new AtomicLong(0);

    public Context pilotCtx;

    public LockWrapper(Lock delegate) {
        this.delegate = delegate;
    }


    @Override
    public void lock() {
        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            delegate.lock();
            if (ticketDispenser.get() > nextServeId.get()) {
                System.out.println("abort pilot execution");
                ThreadManager.cleanupPhantomThreads("");
            }
            ticketDispenser.incrementAndGet();
        } else {
            long myTicket = ticketDispenser.getAndIncrement();
            pilotCtx = Context.current();
            while (nextServeId.get() != myTicket) {
                Thread.yield();
            }
            while(delegateIsLocked.get()){
                Thread.yield();
            }
        }
    }

    @Override
    public void unlock() {
        if (!PilotUtil.isDryRun()) {
            ticketDispenser.decrementAndGet();
            delegateIsLocked.set(false);
            delegate.unlock();
        } else {
            nextServeId.incrementAndGet();
        }
    }

    @Override
    public boolean tryLock() {
        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            boolean result = delegate.tryLock();
            if (result && (ticketDispenser.get() > nextServeId.get())) {
                System.out.println("abort pilot execution");
                //ThreadManager.cleanupPhantomThreads(PilotUtil.getContextKey(pilotCtx));
            }
            if(result){
                ticketDispenser.incrementAndGet();
            }
            return result;
        } else {
            if(delegateIsLocked.get()){
                return false;
            }
            long myTicket = ticketDispenser.getAndIncrement();
            if (nextServeId.get() == myTicket) {
                pilotCtx = Context.current();
                return true;
            } else {
                ticketDispenser.decrementAndGet();
                return false;
            }
        }
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            boolean result = delegate.tryLock(timeout, unit);
            if (result && (ticketDispenser.get() > nextServeId.get())) {
                System.out.println("abort pilot execution");
                //ThreadManager.cleanupPhantomThreads(PilotUtil.getContextKey(pilotCtx));
            }
            return result;
        } else {
            if(delegateIsLocked.get()){
                return false;
            }
            long myTicket = ticketDispenser.getAndIncrement();
            if (nextServeId.get() == myTicket) {
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

            return true;
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (!PilotUtil.isDryRun()) {
            delegateIsLocked.set(true);
            delegate.lockInterruptibly();
            if (ticketDispenser.get() > nextServeId.get()) {
                System.out.println("abort pilot execution");
                //ThreadManager.cleanupPhantomThreads(PilotUtil.getContextKey(pilotCtx));
            }
            ticketDispenser.incrementAndGet();
        } else {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            long myTicket = ticketDispenser.getAndIncrement();
            pilotCtx = Context.current();
            while (nextServeId.get() != myTicket) {
                if (Thread.interrupted()) {
                    ticketDispenser.decrementAndGet();
                    throw new InterruptedException();
                }
                Thread.yield();
            }
            while(delegateIsLocked.get()){
                if (Thread.interrupted()) {
                    ticketDispenser.decrementAndGet();
                    throw new InterruptedException();
                }
                Thread.yield();
            }
        }
    }

    @Override
    public Condition newCondition() {
        return delegate.newCondition();
    }
}