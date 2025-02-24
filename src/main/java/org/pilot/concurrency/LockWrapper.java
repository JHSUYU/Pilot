package org.pilot.concurrency;

import org.pilot.PilotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class LockWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(LockWrapper.class);

    public ReentrantLock lock;
    public boolean modifiedByDryRun = false;

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
