package org.pilot.dev;

import org.pilot.PilotUtil;

public class DevApi {

    public static void printError(){
        PilotUtil.dryRunLog("Error: PilotExecution failed. Please check the logs for more details.");
    }
}
 