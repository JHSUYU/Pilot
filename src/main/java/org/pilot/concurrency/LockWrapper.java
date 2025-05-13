package org.pilot.concurrency;

import org.pilot.PilotUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class LockWrapper implements Lock {
    private final Lock delegate;

    private final AtomicLong ticketDispenser = new AtomicLong(0);

    private final AtomicLong nextServeId = new AtomicLong(0);

    public int holdCounts = 0;

    public LockWrapper(Lock delegate) {
        this.delegate = delegate;
    }

    public  String contextKey(){
        return "";
    }


    @Override
    public void lock() {

        if (!PilotUtil.isDryRun()) {
            if(ticketDispenser.get() > nextServeId.get()){
                System.out.println("abort pilot execution");
            }
            delegate.lock();
            ticketDispenser.incrementAndGet();
        } else {
            if (delegate.tryLock()) {
                delegate.unlock();
            } else {
                System.out.println("abort pilot execution");
                return;
            }

            long myTicket = ticketDispenser.incrementAndGet();
            while (nextServeId.get() != myTicket) {
                Thread.yield();
            }
        }
    }

    @Override
    public void unlock() {
        if (!PilotUtil.isDryRun()) {
            ticketDispenser.decrementAndGet();
            delegate.unlock();
        } else{
            nextServeId.incrementAndGet();
        }
    }

    @Override
    public boolean tryLock() {
        if(!PilotUtil.isDryRun()){
            if(ticketDispenser.get() > nextServeId.get()){
                System.out.println("abort pilot execution");
            }
            return delegate.tryLock();
        } else {
            long myTicket = ticketDispenser.incrementAndGet();
            if (nextServeId.get() == myTicket) {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (!PilotUtil.isDryRun()) {
            return delegate.tryLock(timeout, unit);
        } else {
            long myTicket = ticketDispenser.incrementAndGet();
            if (nextServeId.get() == myTicket) {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (!PilotUtil.isDryRun()) {
            delegate.lockInterruptibly();
        } else {
            long myTicket = ticketDispenser.incrementAndGet();
            while (nextServeId.get() != myTicket) {
                Thread.yield();
            }
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }
}
