package org.pilot.concurrency;

import org.pilot.PilotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConditionVariableWrapper<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionVariableWrapper.class);
    public boolean invokedByDryRun = false;

    public Condition condition;

    public T target;

    public ReentrantLock lock;

    public ConditionVariableWrapper(Condition condition, ReentrantLock lock, T target) {
        this.condition = condition;
        this.lock = lock;
        this.target = target;
    }
    public ConditionVariableWrapper(Condition condition, ReentrantLock lock) {
        this.condition = condition;
        this.lock = lock;
    }

    public void realAwait() throws InterruptedException {
        this.condition.await();
    }

    public void await() throws InterruptedException {
        LOG.info("ConditionVariableWrapper.await()");
        if(PilotUtil.isShadow()){
            LOG.info("ConditionVariableWrapper.await() isShadow");
            PilotUtil.clearBaggage();
            PilotUtil.createDryRunBaggage();
            this.lock.lock();
            return;
        }
        this.condition.await();

        if(invokedByDryRun && PilotUtil.forkCount == 0 && target != null){
            LOG.info("ConditionVariableWrapper createShadowThread invoked by DryRun using reflection");
            this.lock.unlock();
            try {
                Class<?> targetClass = target.getClass();
                Method createShadowThreadMethod = targetClass.getMethod("createShadowThread");
                createShadowThreadMethod.invoke(target);
            } catch (Exception e) {
                LOG.error("Failed to invoke createShadowThread via reflection", e);
            }
            this.realAwait();
        }
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException{
        LOG.info("ConditionVariableWrapper.await(long time, TimeUnit unit)");
        return this.condition.await(time, unit);
    }

    public void signal() {
        LOG.info("ConditionVariableWrapper.signal()");
        if(PilotUtil.isDryRun()){
            invokedByDryRun = true;
        }
        this.condition.signal();
    }
}
