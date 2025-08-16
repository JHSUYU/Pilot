package org.pilot;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.pilot.concurrency.ThreadManager;
import org.pilot.zookeeper.ZooKeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pilot.concurrency.ThreadManager.generatePilotIdFromZooKeeper;

public class PilotUtil
{
    public static int count;
    private static final Logger dryRunLogger = LoggerFactory.getLogger(PilotUtil.class);

    private static Map<String, HashMap<String, WrapContext>> stateMap = new HashMap();

    private static Map<String, HashMap<String, WrapContext>> fieldMap = new HashMap();

    private static final Set<String> dryRunMethods = ConcurrentHashMap.newKeySet();

    public static int forkCount = 0;

    public static boolean debug = false;

    public static boolean verbose = true;
    public static final String DRY_RUN_KEY = "is_dry_run";
    public static final String PILOT_ID_KEY = "pilot_id";
    public static final String FAST_FORWARD_KEY = "is_fast_forward";

    public static final String IS_SHADOW_THREAD_KEY = "is_shadow_thread";

    public static final String SHOULD_RELEASE_LOCK_KEY = "should_release_lock";
    public static ContextKey<Boolean> IS_DRY_RUN = ContextKey.named("is_dry_run");

    public static void dryRunLog(String message) {
        if(!verbose){
            return;
        }
        if(!isDryRun()) {
            dryRunLogger.info(message);
        } else {
            //print stack trace
//            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
//            StringBuilder sb = new StringBuilder();
//            for (StackTraceElement element : stackTrace) {
//                sb.append(element.toString()).append("\n");
//            }
//            dryRunLogger.info("trace in dry run is"+sb.toString());
            dryRunLogger.info(message + " in dry run mode");
        }
    }

//    static {
//        Thread monitorThread = new Thread(() -> {
//            while (!Thread.currentThread().isInterrupted()) {
//                try {
//                    int size = dryRunMethods.size();
//                    try (FileWriter writer = new FileWriter("/opt/invokedDryRunmethods.txt")) {
//                        writer.write(String.valueOf(size));
//                        writer.write(System.lineSeparator());
//                    } catch (IOException e) {
//                    }
//                    Thread.sleep(30000);
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        });
//        monitorThread.setDaemon(true);
//        monitorThread.start();
//    }


    public static boolean isDryRun(String methodSignature){
        boolean res = Boolean.parseBoolean(Baggage.current().getEntryValue(DRY_RUN_KEY));
        if(res){
            dryRunMethods.add(methodSignature);
        }
        return res;
    }


    public static boolean isDryRun() {
        String value = Baggage.current().getEntryValue(DRY_RUN_KEY);
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            int dryRunValue = Integer.parseInt(value);
            return dryRunValue != 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isShadow() {
        if(debug){
            return false;
        }else{
            return Baggage.current().getEntryValue(IS_SHADOW_THREAD_KEY) != null && Boolean.parseBoolean(Baggage.current().getEntryValue(IS_SHADOW_THREAD_KEY));
        }
        //return false;
    }

    public static boolean isFastForward() {
        //print thread information
        boolean res = Baggage.current().getEntryValue(FAST_FORWARD_KEY) != null && Boolean.parseBoolean(Baggage.current().getEntryValue(FAST_FORWARD_KEY));
        System.out.println("Thread: " + Thread.currentThread().getName() + "result is: " + res)  ;
        if(debug){
            return false;
        }else{
            return res;
        }
    }

    public static UUID getContextKey(Context ctx){
        return UUID.randomUUID();
    }

    public static Scope getDryRunTraceScope(boolean needsDryRunTrace) {
        if (!needsDryRunTrace) {
            return Baggage.empty().makeCurrent();
        }
        Baggage dryRunBaggage = Baggage.current().toBuilder()
                .put(DRY_RUN_KEY, "true")
                .build();
        return dryRunBaggage.makeCurrent();
    }

    public static Scope getDryRunTraceScope(int pilotID) {
        // 将 int 值转换为字符串并放入 Baggage
        Baggage dryRunBaggage = Baggage.current().toBuilder()
                .put(DRY_RUN_KEY, String.valueOf(pilotID))
                .build();
        return dryRunBaggage.makeCurrent();
    }

    public static int getPilotID(){
        String pilotId = Baggage.current().getEntryValue(DRY_RUN_KEY);
        if (pilotId == null || pilotId.isEmpty()) {
            return 0; // 或者抛出异常
        }
        try {
            return Integer.parseInt(pilotId);
        } catch (NumberFormatException e) {
            return 0; // 或者抛出异常
        }
    }

    public static Baggage createFastForwardBaggage(boolean flag) {
        if(!flag){
            return null;
        }
        Baggage fastForwardBaggage = Baggage.current().toBuilder().put(FAST_FORWARD_KEY, "true").build();
        fastForwardBaggage.makeCurrent();
        Context.current().with(fastForwardBaggage).makeCurrent();
        return fastForwardBaggage;
    }

    public static Baggage createShadowBaggage() {
        System.out.println("Creating shadow baggage");
        Baggage shadowBaggage = Baggage.current().toBuilder().put(IS_SHADOW_THREAD_KEY, "true").build();
        shadowBaggage.makeCurrent();
        Context.current().with(shadowBaggage).makeCurrent();
        return shadowBaggage;
    }

    public static Baggage createFastForwardBaggage() {
        System.out.println("Creating fast forward baggage");
        Baggage fastForwardBaggage = Baggage.current().toBuilder().put(FAST_FORWARD_KEY, "true").build();
        fastForwardBaggage.makeCurrent();
        Context.current().with(fastForwardBaggage).makeCurrent();
        return fastForwardBaggage;
    }

    public static Baggage createDryRunBaggage() {
        System.out.println("Creating dry run baggage");
        Baggage dryRunBaggage = Baggage.current().toBuilder().put(DRY_RUN_KEY, "true").build();
        dryRunBaggage.makeCurrent();
        Context.current().with(dryRunBaggage).makeCurrent();
        return dryRunBaggage;
    }

    public static void removeDryRunBaggage() {
        Baggage emptyBaggage = Baggage.empty();
        emptyBaggage.makeCurrent();
        Context.current().with(emptyBaggage).makeCurrent();
    }

    public static void clearBaggage() {
        System.out.println("Clearing baggage");
        Baggage emptyBaggage = Baggage.empty();
        emptyBaggage.makeCurrent();
        Context.current().with(emptyBaggage).makeCurrent();
    }

    //print all the information in the stateMap
    public static void printStateMap(){
        System.out.println("Printing state map");
        for(Map.Entry<String, HashMap<String, WrapContext>> entry: stateMap.entrySet()){
            String methodName = entry.getKey();
            Map<String, WrapContext> state = entry.getValue();
            System.out.println("Method: " + methodName);
            for(Map.Entry<String, WrapContext> entry2: state.entrySet()){
                String varName = entry2.getKey();
                WrapContext value = entry2.getValue();
                System.out.println("Variable: " + varName + " Value: " + value.value);
            }
        }
    }

    public static void recordState(String methodSig, HashMap<String, WrapContext> state){
        System.out.println("Recording state for method: " + methodSig);
        HashMap<String, WrapContext> varMap = new HashMap<>();

        for(Map.Entry<String, WrapContext> entry: state.entrySet()){
            String name = entry.getKey();
            //Object value = cloner.deepClone(entry.getValue());
            WrapContext value = entry.getValue();
            //      if(value.value != null){
            //        System.out.println("Value.value classname is: " + value.value.getClass().getName());
            ////        if(shouldBeDeepCloned(value.value.getClass().getName())){
            ////          value.value = cloner.deepClone(value.value);
            ////        }
            //      }
            varMap.put(name, value);
        }

        String tmp = methodSig;
//    if(methodSig.contains("access$")){
//      tmp = methodSig.replaceAll("[0-9]", "");
//    }

        stateMap.put(tmp, varMap);

    }

    public static void recordFieldState(String methodSig, HashMap<String, WrapContext> state){
        System.out.println("Recording field for method: " + methodSig);
        HashMap<String, WrapContext> varMap = new HashMap<>();

        for(Map.Entry<String, WrapContext> entry: state.entrySet()){
            String name = entry.getKey();
            WrapContext value = entry.getValue();
            //System.out.println("Value.value classname is: " + value.value.getClass().getName());
            //      if(shouldBeDeepCloned(value.value.getClass().getName())){
            //        value.value = cloner.deepClone(value.value);
            //      }
            varMap.put(name, value);
        }

        String tmp = methodSig;
//    if(methodSig.contains("access$")){
//      tmp = methodSig.replaceAll("[0-9]", "");
//    }

        fieldMap.put(tmp, varMap);

    }

    public static boolean shouldBeDeepCloned(String className){
        if(className.contains("java.lang.ref.WeakReference") || className.contains("org.apache.hadoop.hbase.io.hfile.LruBlockCache")
                || className.contains("java.util.HashMap") || className.contains("java.util.concurrent.locks.ReentrantLock")
                || className.contains("org.apache.logging.slf4j.Log4jLogger")){
            return false;
        }
        return true;
    }

    public static HashMap<String, WrapContext> getState(String methodSig){
        System.out.println("Getting state for method: " + methodSig);

        String methodNameTmp = methodSig.replace("$shadow","");
        methodNameTmp = methodNameTmp.replace("Shadow","");

//    if(methodNameTmp.contains("access$")){
//      methodNameTmp = methodSig.replaceAll("[0-9]", "");
//    }

        HashMap<String, WrapContext> state = stateMap.get(methodNameTmp);
        //print key, value in state
        System.out.println("Getting state for method: " + methodNameTmp);
        for(Map.Entry<String, WrapContext> entry: state.entrySet()){
            String varName = entry.getKey();
            WrapContext value = entry.getValue();
            System.out.println("Variable: " + varName + " Value: " + value.value);
        }
        return state;
    }

    public static HashMap<String, WrapContext> getFieldState(String methodSig){
        System.out.println("Getting Field state for method: " + methodSig);

        String methodNameTmp = methodSig.replace("$shadow","");
        methodNameTmp = methodNameTmp.replace("Shadow","");

//    if(methodNameTmp.contains("access$")){
//      methodNameTmp = methodSig.replaceAll("[0-9]", "");
//    }

        HashMap<String, WrapContext> state = fieldMap.get(methodNameTmp);
        //print key, value in state
        System.out.println("Getting field for method: " + methodNameTmp);
        for(Map.Entry<String, WrapContext> entry: state.entrySet()){
            String varName = entry.getKey();
            WrapContext value = entry.getValue();
            System.out.println("Getting fieldState Variable: " + varName + " Value: " + value.value);
        }
        return state;
    }

    //  public static void createShadowThread(){
    //    Baggage fastForwardBaggage = Baggage.current().toBuilder()
    //      .put(FAST_FORWARD_KEY, "true")
    //      .build();
    //    Context fastForwardContext = Context.current().with(fastForwardBaggage);
    //    fastForwardContext.makeCurrent();
    //
    //    ExecutorService baseExecutor = new ThreadPoolExecutor(
    //      1,  // corePoolSize
    //      1,  // maximumPoolSize
    //      0L, // keepAliveTime
    //      TimeUnit.MILLISECONDS,  // TimeUnit for keepAliveTime
    //      new LinkedBlockingQueue<>()  // workQueue
    //    );
    //
    //    ShadowMicroFork shadowThread = new ShadowMicroFork(1, true, 1, true);
    //    shadowThread.lock = new ReentrantLock();
    //    Future<?> future = baseExecutor.submit(shadowThread);
    //
    //    try {
    //      future.get();
    //      System.out.println("field3 value after execution: " + ((ShadowMicroFork)shadowThread).field3);
    //    } catch(InterruptedException | ExecutionException e) {
    //      e.printStackTrace();
    //    } finally {
    //      baseExecutor.shutdown();
    //    }
    //
    //  }

    private static final class ExecutionUnit {
        final String methodSignature;
        final String unitId;

        ExecutionUnit(String methodSignature, String unitId) {
            this.methodSignature = methodSignature;
            this.unitId = unitId;
        }
    }

    private static final ConcurrentHashMap<Long, ArrayList<ExecutionUnit>> executionMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long, Long> threadToShadowThreadMap = new ConcurrentHashMap<>();

    private static final ThreadLocal<Integer> index = ThreadLocal.withInitial(() -> 0);

    public static void recordShadowThreadInMap(long originalThreadId, long shadowThreadId) {
        threadToShadowThreadMap.put(shadowThreadId, originalThreadId);
    }

    public static void recordExecutingUnit(String methodSig, String unitId, long threadId) {
        ArrayList<ExecutionUnit> array = executionMap.computeIfAbsent(threadId, k -> new ArrayList<>());
        array.add(new ExecutionUnit(methodSig, unitId));
        System.out.println("Recording executing unit: " + methodSig + " " + unitId);
        Thread.currentThread().getId();
    }

    public static int getExecutingUnit(long threadId) {
        long originalThreadId = threadToShadowThreadMap.get(threadId);
        ArrayList<ExecutionUnit> array = executionMap.get(originalThreadId);
        ExecutionUnit unit = array.get(index.get());
        index.set(index.get() + 1);
        System.out.println("Getting executing unit: " + unit.methodSignature + " " + unit.unitId);
        return Integer.parseInt(unit.unitId);
    }

    public static void popExecutingUnit(long originalThreadId) {
        ArrayList<ExecutionUnit> array = executionMap.get(originalThreadId);
        array.remove(array.size()-1);
        System.out.println("Popping executing unit, remaining size: " + array.size());
        //print all the remaining units
        for(ExecutionUnit unit: array){
            System.out.println("Remaining unit: " + unit.methodSignature + " " + unit.unitId);
        }
    }

    public static boolean shouldBeContextWrap(Runnable runnable, Executor executor) {
//        dryRunLog("Checking if should be context wrapped");
//        dryRunLog("Runnable class is: " + runnable.getClass().getName() + " executor class is: " + executor.getClass().getName());
//        dryRunLog("isDryRun is: " + isDryRun());
        if(executor.getClass().getName().contains("SEPExecutor")) {
            //dryRunLog("should not be context wrapped");
            return false;
        }
        return true;
    }

    public static void addWorkerThread(Thread thread) {
        sedaWorkerThreads.add(thread);
        dryRunLog("Added worker thread: " + thread.getName() + ", total threads: " + sedaWorkerThreads.size());
    }

    public static int initNewExec(){
        int executionId = Integer.parseInt(generatePilotIdFromZooKeeper());
        dryRunLog("Starting pilot execution with ID: " + executionId);
        return executionId;
    }
    public static void waitUntilPilotExecutionFinished(Context ctx) {
        final long TIMEOUT_THRESHOLD = 300000;
        final long POLL_INTERVAL = 1000;

        try {
            Baggage baggage = Baggage.fromContext(ctx);
            if (baggage == null) {
                dryRunLog("No baggage found in context");
                return;
            }

            String pilotId = baggage.getEntryValue(PILOT_ID_KEY);
            if (pilotId == null || pilotId.isEmpty()) {
                dryRunLog("No PILOT_ID found in baggage");
                return;
            }

            dryRunLog("Waiting for pilot execution to finish: " + pilotId);
            String pilotNodePath = ThreadManager.PILOT_PATH + "/" + pilotId;

            ZooKeeperClient zkClient = ThreadManager.getZooKeeperClient();
            if (zkClient == null) {
                dryRunLog("ZooKeeper client is not available");
                return;
            }

            // 检查主节点是否存在
            if (!zkClient.exists(pilotNodePath)) {
                dryRunLog("Pilot node doesn't exist: " + pilotId);
                return;
            }

            long startTime = System.currentTimeMillis();
            boolean timeoutReached = false;


            while (true) {
                try {
                    if (!zkClient.exists(pilotNodePath)) {
                        dryRunLog("Pilot node was deleted by another process: " + pilotId);
                        break;
                    }

                    List<String> children = zkClient.zk.getChildren(pilotNodePath, false);

                    if (children == null || children.isEmpty()) {
                        dryRunLog("All children removed for pilot node: " + pilotId);
                        break;
                    }

                    dryRunLog("Pilot node " + pilotId + " still has " + children.size() + " children: " + children);

                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= TIMEOUT_THRESHOLD) {
                        dryRunLog("Timeout reached for pilot execution: " + pilotId +
                                ", elapsed time: " + elapsedTime + "ms");
                        timeoutReached = true;

                        break;
                    }

                    Thread.sleep(POLL_INTERVAL);

                } catch (InterruptedException e) {
                    dryRunLog("Sleep interrupted: " + e.getMessage());
                    Thread.currentThread().interrupt();
                    break;
                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                    dryRunLog("Pilot node no longer exists: " + pilotId);
                    break;
                } catch (Exception e) {
                    dryRunLog("Error checking pilot node children: " + e.getMessage());
                    e.printStackTrace();
                    try {
                        Thread.sleep(POLL_INTERVAL);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            // 记录执行时间
            long totalTime = System.currentTimeMillis() - startTime;
            dryRunLog("Pilot execution " + pilotId + " finished. Total wait time: " +
                    totalTime + "ms" + (timeoutReached ? " (timeout)" : " (completed)"));

            deletePilotNode(zkClient, pilotNodePath, pilotId);


        } catch (Exception e) {
            dryRunLog("Error in waitUntilPilotExecutionFinished: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 删除pilot节点及其所有子节点
     */
    private static void deletePilotNode(ZooKeeperClient zkClient, String pilotNodePath, String pilotId) {
        try {
            if (!zkClient.exists(pilotNodePath)) {
                dryRunLog("Pilot node already deleted: " + pilotId);
                return;
            }

            // 先删除所有子节点
            try {
                List<String> remainingChildren = zkClient.zk.getChildren(pilotNodePath, false);
                if (remainingChildren != null && !remainingChildren.isEmpty()) {
                    dryRunLog("Deleting " + remainingChildren.size() + " remaining children for pilot node: " + pilotId);
                    for (String child : remainingChildren) {
                        String childPath = pilotNodePath + "/" + child;
                        try {
                            zkClient.delete(childPath);
                            dryRunLog("Deleted child node: " + childPath);
                        } catch (Exception e) {
                            dryRunLog("Error deleting child node " + childPath + ": " + e.getMessage());
                        }
                    }
                }
            } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                dryRunLog("Pilot node was already deleted: " + pilotId);
                return;
            } catch (Exception e) {
                dryRunLog("Error getting children for deletion: " + e.getMessage());
            }

            // 删除主节点
            try {
                zkClient.delete(pilotNodePath);
                dryRunLog("Deleted pilot node: " + pilotId);
            } catch (Exception e) {
                dryRunLog("Error deleting pilot node: " + e.getMessage());
                e.printStackTrace();
            }

        } catch (Exception e) {
            dryRunLog("Error in deletePilotNode: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void recordTime(long startTime, String path) {
        try {
            File file = new File(path);
            // Create parent directories if they don't exist
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            // Use FileWriter with append mode (true)
            FileWriter writer = new FileWriter(file, true);
            writer.write(String.valueOf(startTime));
            // 可以添加换行符，使每条记录独占一行
            writer.write(System.lineSeparator());
            writer.close();
        } catch (IOException e) {
            dryRunLog("Error writing start time to file: " + e.getMessage());
        }
    }



    private static final ConcurrentHashMap<String, Long> executionStartTimes = new ConcurrentHashMap<>();
    public static final AtomicLong executionIdGenerator = new AtomicLong(0);

    public static List<Thread> sedaWorkerThreads = new ArrayList<>();

    public static final String mock = "mock";


    private static int generateExecutionId() {
        //replace this with Zookeeper to get increasing ID globally
        long id = executionIdGenerator.incrementAndGet();
        return (int) id;
    }



    public static long getExecutionRuntime(String executionId) {
        Long startTime = executionStartTimes.get(mock);
        if (startTime == null) {
            return -1;
        }
        return (System.currentTimeMillis() - startTime) / 1000;
    }

    public static void print(){
        if(PilotUtil.isDryRun()){
            System.out.println("Dry run mode is enabled, printing stack trace");
            Exception e = new Exception("Dry run mode is enabled, printing stack trace");
            e.printStackTrace();
        }

    }


}
