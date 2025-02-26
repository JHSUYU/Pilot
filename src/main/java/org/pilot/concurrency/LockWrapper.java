package org.pilot.concurrency;

import org.pilot.PilotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.locks.ReentrantLock;

public class LockWrapper<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LockWrapper.class);

    public ReentrantLock lock;
    public boolean modifiedByDryRun = false;

    public T target;

    public LockWrapper(ReentrantLock lock) {
        this.lock = lock;
    }

    public void realLock() {
        this.lock.lock();
    }

    public void realUnlock() {
        this.lock.unlock();
    }

    public void lock() {
        if(PilotUtil.isShadow()){
            LOG.info("LockWrapper.lock() isShadow");
            PilotUtil.clearBaggage();
            PilotUtil.createDryRunBaggage();
        }

        this.lock.lock();

        if(modifiedByDryRun && PilotUtil.forkCount == 0 && target != null){
            LOG.info("ConditionVariableWrapper createShadowThread invoked by DryRun using reflection");
            this.lock.unlock();
            try {
                Class<?> targetClass = target.getClass();
                Method createShadowThreadMethod = targetClass.getMethod("createShadowThread");
                createShadowThreadMethod.invoke(target);
            } catch (Exception e) {
                LOG.error("Failed to invoke createShadowThread via reflection", e);
            }
            this.lock.lock();

        }
    }

    public void unlock() {
        LOG.info("Release DryRun lock");
        if(PilotUtil.isDryRun()){
            LOG.info("Release DryRun Lock");
            this.modifiedByDryRun = true;
        }
        this.lock.unlock();
    }
}
