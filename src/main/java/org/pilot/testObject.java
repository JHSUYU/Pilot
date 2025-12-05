package org.pilot;

import io.opentelemetry.context.Context;

public class testObject {
    public Context PilotContext;

    public void info(){
        System.out.println("testObject" + PilotUtil.isDryRun(PilotContext));
    }
}
