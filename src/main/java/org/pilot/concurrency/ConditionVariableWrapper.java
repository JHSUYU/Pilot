package org.pilot.concurrency;

import io.opentelemetry.context.Context;
import org.pilot.PilotUtil;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

public class ConditionVariableWrapper implements Condition {
    private Condition delegate;
    private Lock associatedLock;

    private static class WaitNode{
        Thread thread;
        boolean isPhantom;
        WaitNode(Thread thread, boolean isPhantom){
            this.thread = thread;
            this.isPhantom = isPhantom;
        }
    }

    private Queue<WaitNode> queue = new ConcurrentLinkedQueue<>();

    public ConditionVariableWrapper(Condition delegate, Lock lock) {
        this.delegate = delegate;
        this.associatedLock = lock;
    }

    @Override
    public void await() throws InterruptedException {
        if(!PilotUtil.isDryRun()){
            queue.add(new WaitNode(Thread.currentThread(), false));
            delegate.await();
        }else{
            Thread me = Thread.currentThread();
            queue.add(new WaitNode(me, true));
            associatedLock.unlock();
            LockSupport.park(this);
            associatedLock.lock();
        }
    }

    @Override
    public void signal() {
        if(!PilotUtil.isDryRun()){
            delegate.signal();
        }else{
            WaitNode node = queue.poll();
            if(node == null){
                return;
            }

            if(!node.isPhantom){
                System.out.println("Micro Fork");
            }else{
                LockSupport.unpark(node.thread);
            }
        }
    }

    @Override
    public void signalAll() {
        if(!PilotUtil.isDryRun()){
            delegate.signalAll();
        }else {
            WaitNode node;
            while((node = queue.poll()) != null){
                if(!node.isPhantom){
                    System.out.println("Micro Fork");
                }else{
                    LockSupport.unpark(node.thread);
                }
            }
        }
    }


    @Override public void awaitUninterruptibly()               { throw new UnsupportedOperationException(); }
    @Override public long awaitNanos(long nanosTimeout)        { throw new UnsupportedOperationException(); }
    @Override public boolean await(long time, TimeUnit unit)    { throw new UnsupportedOperationException(); }
    @Override public boolean awaitUntil(java.util.Date deadline){ throw new UnsupportedOperationException(); }
}
