package org.pilot.dev;

import org.pilot.PilotUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.pilot.PilotUtil.recordTime;

public class DevApi {

    public static void printError(){
        long detectTime = System.currentTimeMillis();
        //recordTime(detectTime,"/users/ZhenyuLi/detecttime.txt");
        PilotUtil.dryRunLog("Error: PilotExecution failed. Please check the logs for more details. Detect use " + PilotUtil.getExecutionRuntime("") + "." );
    }

}
 