package org.pilot;

import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.collect.*;
import com.rits.cloning.Cloner;
import com.rits.cloning.ICloningStrategy;

import static org.pilot.Constants.SHADOW_DIR;

public class State {

    public static final String[] fileSystemClasses = {
            "sun.nio.fs.UnixPath",
            "sun.nio.fs.WindowsPath",
            "sun.nio.fs.UnixFileSystem",
            "sun.nio.fs.WindowsFileSystem",
            "sun.nio.fs.UnixFileSystemProvider",
            "sun.nio.fs.WindowsFileSystemProvider",
            "java.nio.file.Path",
            "java.io.File",
            "java.io.RandomAccessFile",
            "java.nio.channels.FileChannel",
            "java.nio.channels.FileLock"
    };

    public static final String[] luceneDirectoryClasses = {
            "org.apache.lucene.store.Directory",
            "org.apache.lucene.store.FSDirectory",
            "org.apache.lucene.store.MMapDirectory",
            "org.apache.lucene.store.NIOFSDirectory",
            "org.apache.lucene.store.SimpleFSDirectory",
            "org.apache.lucene.store.RAMDirectory",
            "org.apache.lucene.store.ByteBuffersDirectory",
            "org.apache.lucene.store.FilterDirectory",
            "org.apache.lucene.store.TrackingDirectoryWrapper",
            "org.apache.lucene.store.LockValidatingDirectoryWrapper",
            "org.apache.lucene.store.Lock",
            "org.apache.lucene.store.LockFactory",
            "org.apache.lucene.store.IndexInput",
            "org.apache.lucene.store.IndexOutput",
            "org.apache.lucene.store.IOContext"
    };

    public static final String[] lockingClasses = {
            "java.util.concurrent.locks.Lock",
            "java.util.concurrent.locks.ReentrantLock",
            "java.util.concurrent.locks.ReadWriteLock",
            "java.util.concurrent.locks.ReentrantReadWriteLock",
            "java.util.concurrent.Semaphore",
            "java.util.concurrent.CountDownLatch",
            "java.util.concurrent.CyclicBarrier",
            "java.util.concurrent.Phaser",
            "java.lang.Object", // for synchronized blocks
            "org.apache.lucene.index.DocumentsWriterFlushQueue",
            "org.apache.lucene.index.DocumentsWriterDeleteQueue"
    };

    public static final String[] threadingClasses = {
            "java.lang.Thread",
            "java.lang.ThreadGroup",
            "java.util.concurrent.ThreadPoolExecutor",
            "java.util.concurrent.ExecutorService",
            "java.util.concurrent.Executor",
            "java.util.concurrent.ScheduledExecutorService",
            "java.util.concurrent.ForkJoinPool",
            "org.apache.lucene.index.MergeScheduler",
            "org.apache.lucene.index.ConcurrentMergeScheduler",
            "org.apache.lucene.index.SerialMergeScheduler"
    };

    public static final String[] configClasses = {
            "org.apache.lucene.util.InfoStream",
            "org.apache.lucene.index.IndexWriterConfig",
            "org.apache.lucene.index.LiveIndexWriterConfig",
            "org.apache.lucene.index.MergePolicy",
            "org.apache.lucene.index.IndexDeletionPolicy",
            "org.apache.lucene.index.FlushPolicy",
            "org.apache.lucene.index.FieldInfos$FieldNumbers",
            "org.apache.lucene.analysis.Analyzer",
            "org.apache.lucene.codecs.Codec",
            "org.apache.lucene.search.similarities.Similarity"
    };

    public static final String[] callbackClasses = {
            "org.apache.lucene.index.IndexWriter$Event",
            "org.apache.lucene.index.IndexWriter$EventQueue",
            "org.apache.lucene.index.IndexWriter$IndexReaderWarmer",
            "org.apache.lucene.index.SegmentInfos$FindSegmentsFile",
            "org.apache.lucene.index.IndexReader$CacheHelper",
            "org.apache.lucene.index.IndexReader$ClosedListener",
            "org.apache.lucene.index.QueryTimeout",
            "org.apache.lucene.index.DocumentsWriter$FlushNotifications"
    };

    public static final String[] poolingClasses = {
            "org.apache.lucene.index.ReaderPool",
            "org.apache.lucene.index.ReadersAndUpdates",
            "org.apache.lucene.index.BufferedUpdatesStream",
            "org.apache.lucene.index.SegmentReader",
            //"org.apache.lucene.index.StandardDirectoryReader",
            "org.apache.lucene.util.Accountable",
            "org.apache.lucene.util.ByteBlockPool",
            "org.apache.lucene.util.RecyclingByteBlockAllocator"
    };

    public static final String[] atomicClasses = {
            "java.util.concurrent.ConcurrentLinkedQueue",
            "java.util.concurrent.ConcurrentHashMap"
    };

    public static final String[] testingClasses = {
            "com.carrotsearch.randomizedtesting.ThreadLeakControl",
            "com.carrotsearch.randomizedtesting.RandomizedRunner",
            "com.carrotsearch.randomizedtesting.rules.StatementAdapter",
            "org.apache.lucene.util.TestRule",
            "junit.framework.TestCase"
    };


    public static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(State.class);

    public static Cloner cloner = new Cloner();



    static {

        for (String[] classArray : new String[][]{
                fileSystemClasses, luceneDirectoryClasses, lockingClasses,
                threadingClasses, configClasses, callbackClasses,
                poolingClasses, atomicClasses, testingClasses}) {
            for (String className : classArray) {
                try {
                    Class<?> clazz = Class.forName(className);
                    cloner.dontClone(clazz);
                } catch (ClassNotFoundException e) {
                    // 类不存在，忽略
                }
            }
        }

        cloner.dontCloneInstanceOf(Function.class);
        cloner.dontCloneInstanceOf(Runnable.class);
        cloner.dontCloneInstanceOf(Callable.class);
        cloner.dontCloneInstanceOf(Consumer.class);
        cloner.dontCloneInstanceOf(Supplier.class);
        cloner.dontCloneInstanceOf(Predicate.class);


        cloner.registerCloningStrategy(new ICloningStrategy() {
            @Override
            public Strategy strategyFor(Object obj, Field field) {
                if (field == null || field.getType() == null) {
                    return Strategy.IGNORE;
                }

                String fieldTypeName = field.getType().getName();
                String fieldName = field.getName();


                // 7. 检查字段值的实际类型
                try {
                    field.setAccessible(true);
                    Object value = field.get(obj);
                    if (value != null) {
                        String valueClassName = value.getClass().getName();

                        // 文件系统相关
                        if (valueClassName.startsWith("sun.nio.fs.") ||
                                valueClassName.startsWith("java.nio.file.") ||
                                valueClassName.startsWith("org.apache.lucene.store.")) {
                            return Strategy.SAME_INSTANCE_INSTEAD_OF_CLONE;
                        }

                        if (valueClassName.contains("Lambda") ||
                                valueClassName.contains("$Lambda") ||
                                value.getClass().isSynthetic()) {
                            //LOG.debug("Skipping lambda field: {} in class: {}", fieldName, obj.getClass().getName());
                            return Strategy.SAME_INSTANCE_INSTEAD_OF_CLONE;
                        }

                        if(valueClassName.contains("org.apache.solr")){
                            return Strategy.SAME_INSTANCE_INSTEAD_OF_CLONE;
                        }

                        // 检查是否是匿名内部类（也可能包含不可克隆的引用）
                        if (value.getClass().isAnonymousClass()) {
                            //LOG.debug("Skipping anonymous class field: {} in class: {}", fieldName, obj.getClass().getName());
                            return Strategy.SAME_INSTANCE_INSTEAD_OF_CLONE;
                        }



                        // 线程相关
                        if (value instanceof Thread || value instanceof ThreadGroup ||
                                value instanceof ExecutorService) {
                            return Strategy.SAME_INSTANCE_INSTEAD_OF_CLONE;
                        }
                    }
                } catch (IllegalAccessException e) {
                    // 忽略访问错误
                }

                return Strategy.IGNORE;
            }
        });
    }

    public static IOManager IOManager = new IOManager();
    public static <T> T shallowCopy(T originalField, T dryRunField, boolean needsSet){
        if (!needsSet) {
            return dryRunField;
        } else {
            return clone(originalField);
        }
    }


    public static <T> T deepCopy(T obj) {
        return cloner.deepClone(obj);
    }

    public static <T> boolean workaroundForSolr(T obj){
        if(obj.getClass().getName().contains("org.apache.lucene.store")){
            return false;
        }

        if(obj.getClass().getName().contains("org.apache.lucene")){
            LOG.info("Workaround for Solr: Using deep copy for object of type: {}", obj.getClass().getName());
            return true;
        }
        return false;
    }


    public static <T> T clone(T obj) {
        if (obj == null) {
            return null;
        }



        //if obj instance of AtomicPrimitive, do deep copy
        if (obj instanceof AtomicInteger) {
            return (T) new AtomicInteger(((AtomicInteger) obj).get());
        } else if (obj instanceof AtomicLong) {
            return (T) new AtomicLong(((AtomicLong) obj).get());
        } else if (obj instanceof AtomicBoolean) {
            return (T) new AtomicBoolean(((AtomicBoolean) obj).get());
        } else if (obj instanceof AtomicReference) {
            return (T) new AtomicReference<>(((AtomicReference<?>) obj).get());
        }

        if(workaroundForSolr(obj)){
            //print stack trace
            for(StackTraceElement element : Thread.currentThread().getStackTrace()) {
                LOG.info("Workaround for Solr: Stack trace: {}", element);
            }
            // catch the throwable from deepCopy, if throwable, just return obj
            try {
                return deepCopy(obj);
            } catch (Throwable e) {
                LOG.error("Failed to deep copy object of type: {}", obj.getClass().getName(), e);
                return obj; // Return original object if deep copy fails
            }
        }

        if(workaroundForCA(obj)){
            try {
                return deepCopy(obj);
            } catch (Throwable e) {
                LOG.error("Failed to deep copy object of type: {}", obj.getClass().getName(), e);
                return obj; // Return original object if deep copy fails
            }
        }


        if (obj instanceof java.util.Properties){
            java.util.Properties originalProperties = (java.util.Properties) obj;
            java.util.Properties clonedProperties = new java.util.Properties();
            for (String key : originalProperties.stringPropertyNames()) {
                clonedProperties.setProperty(key, originalProperties.getProperty(key));
            }
            return (T) clonedProperties;
        }

        if (obj instanceof Set) {
            return (T) cloneSet((Set<?>) obj);
        } else if (obj instanceof Map) {
            return (T) cloneMap((Map<?, ?>) obj);
        } else if (obj instanceof Queue) {
            return (T) cloneQueue((Queue<?>) obj);
        } else if (obj instanceof List) {
            return (T) cloneList((List<?>) obj);
        } else if (obj instanceof Thread) {
            return (T) cloneThread((Thread) obj);
        } else if (obj instanceof FileChannel){
            return (T)IOManager.handleFileChannel((FileChannel) obj);
        } else if (obj instanceof FileOutputStream){
            return (T)IOManager.handleFileOutputStream((FileOutputStream) obj);
        }
//        else if(obj needs workaround) {
//            return deepclone(obj)
//        }
        return obj;
    }




    private Path createShadowPath(Path originalPath) {
        // Create shadow path maintaining the original directory structure
        return Paths.get(originalPath.getRoot().toString(),
                SHADOW_DIR,
                originalPath.subpath(0, originalPath.getNameCount()).toString());
    }

    private String extractPath(FileChannel fileChannel) {
        try {
            for (Field field : fileChannel.getClass().getDeclaredFields()) {
                if (field.getName().equals("path")) {
                    field.setAccessible(true);
                    return (String) field.get(fileChannel);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //

    private static <E> Set<E> cloneSet(Set<E> original) {
        if (original == null) {
            return null;
        }

        // Handle Guava immutable sets
        if (original instanceof ImmutableSortedSet) {
            ImmutableSortedSet<E> sortedSet = (ImmutableSortedSet<E>) original;
            return ImmutableSortedSet.copyOf(sortedSet.comparator(), sortedSet);
        } else if (original instanceof ImmutableSet) {
            return ImmutableSet.copyOf(original);
        }

        // Handle NavigableSet implementations (must come before SortedSet)
        if (original instanceof NavigableSet) {
            NavigableSet<E> navigableSet = (NavigableSet<E>) original;

            if (original instanceof TreeSet) {
                TreeSet<E> originalTS = (TreeSet<E>) original;
                TreeSet<E> newTS = new TreeSet<>(originalTS.comparator());
                newTS.addAll(original);
                return newTS;
            } else if (original instanceof ConcurrentSkipListSet) {
                ConcurrentSkipListSet<E> originalCSLS = (ConcurrentSkipListSet<E>) original;
                ConcurrentSkipListSet<E> newCSLS = new ConcurrentSkipListSet<>(originalCSLS.comparator());
                newCSLS.addAll(original);
                return newCSLS;
            } else {
                // For other NavigableSet implementations, use TreeSet
                TreeSet<E> newTS = new TreeSet<>(navigableSet.comparator());
                newTS.addAll(original);
                return newTS;
            }
        }

        // Handle SortedSet (but not NavigableSet)
        if (original instanceof SortedSet) {
            SortedSet<E> sortedSet = (SortedSet<E>) original;
            TreeSet<E> newTS = new TreeSet<>(sortedSet.comparator());
            newTS.addAll(original);
            return newTS;
        }

        // Handle other Set implementations
        if (original instanceof LinkedHashSet) {
            return new LinkedHashSet<>(original);
        } else if (original instanceof CopyOnWriteArraySet) {
            return new CopyOnWriteArraySet<>(original);
        } else if (original instanceof HashSet) {
            return new HashSet<>(original);
        } else if (original instanceof EnumSet) {
            // Handle EnumSet specially
            @SuppressWarnings("unchecked")
            EnumSet<? extends Enum<?>> enumSet = (EnumSet<? extends Enum<?>>) original;
            return (Set<E>) EnumSet.copyOf(enumSet);
        } else {
            // For unknown types, including Collections.unmodifiableSet,
            // Collections.synchronizedSet, etc.
            // Try to preserve ordering if possible
            try {
                // Check if it's actually a NavigableSet wrapped by Collections.unmodifiableSet
                if (original.getClass().getName().contains("UnmodifiableNavigableSet") ||
                        original.getClass().getName().contains("SynchronizedNavigableSet")) {
                    // Use TreeSet to preserve navigation capabilities
                    return new TreeSet<>(original);
                }
            } catch (Exception e) {
                // Ignore and fall back to HashSet
            }

            // Default to HashSet for unknown types
            return new HashSet<>(original);
        }
    }

    private static <E extends Enum<E>, V> Map<E, V> cloneEnumMap(EnumMap<E, V> original) {
        if (original == null) {
            return null;
        }
        return new EnumMap<>(original);
    }

    public static<T> boolean workaroundForCA(T obj) {
        if(obj.getClass().getName().contains("com.codahale.metrics")){
            return true;
        }
        return false;
    }

    private static <K, V> Map<K, V> cloneMap(Map<K, V> original) {
        if (original == null) {
            return null;
        }

        if(original.getClass().getName().contains("org.cliffc.high_scale_lib.NonBlockingHashMap")){
            return original;
        }

        if(!workaroundForCA(original)){
            return original;
        }

        // Handle Guava immutable maps
        if (original instanceof ImmutableBiMap) {
            return ImmutableBiMap.copyOf(original);
        } else if (original instanceof ImmutableSortedMap) {
            return ImmutableSortedMap.copyOf((ImmutableSortedMap<K, V>) original);
        } else if (original instanceof ImmutableMap) {
            return ImmutableMap.copyOf(original);
        }

        // Handle BiMap implementations
        if (original instanceof BiMap) {
            BiMap<K, V> biMap = (BiMap<K, V>) original;
            if (biMap instanceof HashBiMap) {
                return HashBiMap.create(biMap);
            } else if (biMap instanceof EnumBiMap) {
                // EnumBiMap requires special handling with proper type bounds
                @SuppressWarnings({"unchecked", "rawtypes"})
                EnumBiMap<?, ?> enumBiMap = (EnumBiMap) biMap;
                return (Map<K, V>) EnumBiMap.create(enumBiMap);
            } else if (biMap instanceof EnumHashBiMap) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                EnumHashBiMap<?, ?> enumHashBiMap = (EnumHashBiMap) biMap;
                return (Map<K, V>) EnumHashBiMap.create(enumHashBiMap);
            } else {
                // For other BiMap implementations, use HashBiMap
                return HashBiMap.create(biMap);
            }
        }

        // Handle NavigableMap implementations (must come before SortedMap)
        if (original instanceof NavigableMap) {
            NavigableMap<K, V> navigableMap = (NavigableMap<K, V>) original;

            if (original instanceof TreeMap) {
                TreeMap<K, V> treeMap = (TreeMap<K, V>) original;
                return new TreeMap<>(treeMap);
            } else if (original instanceof ConcurrentSkipListMap) {
                ConcurrentSkipListMap<K, V> skipListMap = (ConcurrentSkipListMap<K, V>) original;
                ConcurrentSkipListMap<K, V> newMap = new ConcurrentSkipListMap<>(skipListMap.comparator());
                newMap.putAll(original);
                return newMap;
            } else {
                // For other NavigableMap implementations, use TreeMap
                TreeMap<K, V> newMap = new TreeMap<>(navigableMap.comparator());
                newMap.putAll(original);
                return newMap;
            }
        }

        // Handle SortedMap (but not NavigableMap)
        if (original instanceof SortedMap) {
            SortedMap<K, V> sortedMap = (SortedMap<K, V>) original;
            TreeMap<K, V> newMap = new TreeMap<>(sortedMap.comparator());
            newMap.putAll(original);
            return newMap;
        }

        // Handle ConcurrentMap implementations
        if (original instanceof ConcurrentHashMap) {
            return new ConcurrentHashMap<>(original);
        } else if (original instanceof ConcurrentMap) {
            // For other ConcurrentMap implementations
            return new ConcurrentHashMap<>(original);
        }

        // Handle other java.util map types
        if (original instanceof LinkedHashMap) {
            return new LinkedHashMap<>(original);
        } else if (original instanceof EnumMap) {
            @SuppressWarnings("unchecked")
            EnumMap<? extends Enum<?>, V> enumMap = (EnumMap<? extends Enum<?>, V>) original;
            return cloneEnumMap((EnumMap) enumMap);
        } else if (original instanceof IdentityHashMap) {
            return new IdentityHashMap<>(original);
        } else if (original instanceof WeakHashMap) {
            return new WeakHashMap<>(original);
        } else if (original instanceof HashMap) {
            return new HashMap<>(original);
        } else if (original instanceof Hashtable) {
            return new Hashtable<>(original);
        } else {
            // For unknown types, including Collections.unmodifiableMap,
            // Collections.synchronizedMap, etc.

            // Try to detect wrapped NavigableMap or SortedMap
            try {
                String className = original.getClass().getName();
                if (className.contains("UnmodifiableNavigableMap") ||
                        className.contains("SynchronizedNavigableMap") ||
                        className.contains("CheckedNavigableMap")) {
                    // Use TreeMap to preserve navigation capabilities
                    return new TreeMap<>(original);
                } else if (className.contains("UnmodifiableSortedMap") ||
                        className.contains("SynchronizedSortedMap") ||
                        className.contains("CheckedSortedMap")) {
                    // Use TreeMap to preserve sorting capabilities
                    return new TreeMap<>(original);
                }
            } catch (Exception e) {
                // Ignore and fall back to HashMap
            }

            System.out.println("Failure Recovery: DryRunManager.java: cloneMap: unknown type " + original.getClass().getName());
            // Default to HashMap for unknown types
            return new HashMap<>(original);
        }
    }

    private static <E> Queue<E> cloneQueue(Queue<E> original) {
        if (original == null) {
            return null;
        }

        if (original instanceof MinMaxPriorityQueue) {
            MinMaxPriorityQueue<E> mmQueue = (MinMaxPriorityQueue<E>) original;
            MinMaxPriorityQueue<E> newQueue = (MinMaxPriorityQueue<E>) MinMaxPriorityQueue.create();
            newQueue.addAll(mmQueue);
            return newQueue;
        } else if (original instanceof EvictingQueue) {
            EvictingQueue<E> evictingQueue = (EvictingQueue<E>) original;
            EvictingQueue<E> newQueue = EvictingQueue.create(evictingQueue.remainingCapacity() + evictingQueue.size());
            newQueue.addAll(evictingQueue);
            return newQueue;
        }

        if (original instanceof PriorityQueue) {
            return new PriorityQueue<>((PriorityQueue<E>) original);
        } else if (original instanceof ConcurrentLinkedQueue) {
            return new ConcurrentLinkedQueue<>(original);
        } else if (original instanceof BlockingQueue) {
            if (original instanceof ArrayBlockingQueue) {
                ArrayBlockingQueue<E> abq = (ArrayBlockingQueue<E>) original;
                return new ArrayBlockingQueue<>(abq.size(), abq.remainingCapacity() == 0, original);
            } else if (original instanceof LinkedBlockingQueue) {
                return new LinkedBlockingQueue<>(original);
            } else if (original instanceof LinkedBlockingDeque) {
                return new LinkedBlockingDeque<>(original);
            } else if (original instanceof PriorityBlockingQueue) {
                return new PriorityBlockingQueue<>(original);
            }
        } else if (original instanceof Deque) {
            if (original instanceof ConcurrentLinkedDeque) {
                return new ConcurrentLinkedDeque<>(original);
            } else if (original instanceof LinkedBlockingDeque) {
                return new LinkedBlockingDeque<>(original);
            } else {
                return new LinkedList<>(original);
            }
        }
        return new LinkedList<>(original);
    }

    private static <E> List<E> cloneList(List<E> original) {
        if (original == null) {
            return null;
        }

        if (original instanceof ImmutableList) {
            return ImmutableList.copyOf(original);
        }
        //if java.util.Vector
        if (original instanceof Stack){
            //put original to stack
            Stack<E> stack = new Stack<>();
            stack.addAll(original);
            return stack;
        }
        if (original instanceof Vector) {
            return new Vector<>(original);
        }

        if (original instanceof LinkedList) {
            return new LinkedList<>(original);
        } else if (original instanceof CopyOnWriteArrayList) {
            return new CopyOnWriteArrayList<>(original);
        } else {
            return new ArrayList<>(original);
        }
    }

    private static Thread cloneThread(Thread original) {
        if (original == null) {
            return null;
        }

        /*Cloner cloner = new Cloner();
        cloner.dontClone(Context.class); // Skip cloning Context
	cloner.dontClone(Baggage.class); // Skip other OTEL classes if needed

	try {
            return cloner.deepClone(original);
        } catch (Exception e) {
            System.err.println("Failed to clone Thread: " + e.getMessage());
            e.printStackTrace();
            return null;
        }*/
        try {
            // Use reflection to access the private "target" field (the Runnable)
            Field targetField = Thread.class.getDeclaredField("target");
            targetField.setAccessible(true); // Bypass private access
            Runnable target = (Runnable) targetField.get(original);
            Thread clone = new Thread(target);
            clone.setName(original.getName());
            clone.setPriority(original.getPriority());
            clone.setDaemon(original.isDaemon());
            clone.setUncaughtExceptionHandler(original.getUncaughtExceptionHandler());
            clone.setContextClassLoader(original.getContextClassLoader());

            return clone;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            System.err.println("Failed to clone Thread: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
