package org.pilot;

import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

import static org.pilot.Constants.SHADOW_DIR;

public class State {
    public static <T> T shallowCopy(T originalField, T dryRunField, boolean isSet){
        if (isSet) {
            return dryRunField;
        } else {
            return clone(originalField);
        }
    }

    public static <T> T clone(T obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Set) {
            return (T) cloneSet((Set<?>) obj);
        } else if (obj instanceof Map) {
            return (T) cloneMap((Map<?, ?>) obj);
        } else if (obj instanceof Queue) {
            return (T) cloneQueue((Queue<?>) obj);
        } else if (obj instanceof List) {
            return (T) cloneList((List<?>) obj);
        } else if (obj instanceof FileChannel)
//        else if(obj needs workaround) {
//            return deepclone(obj)
//        }
            return obj;
    }

    public FileChannel handleFileChannel(FileChannel fileChannel) {
        try {
            // Get original file path using reflection
            String originalPath = extractPath(fileChannel);
            if (originalPath == null) {
                throw new IllegalStateException("Could not extract path from file channel");
            }

            // Create shadow path
            Path originalFilePath = Paths.get(originalPath);
            Path shadowPath = createShadowPath(originalFilePath);

            // Create shadow directory if it doesn't exist
            Files.createDirectories(shadowPath.getParent());

            // Check if shadow file exists, if not, copy the original
            if (!Files.exists(shadowPath)) {
                Files.copy(originalFilePath, shadowPath);
            }

            // Open and store the shadow file channel
            FileChannel shadowChannel = FileChannel.open(shadowPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            // Store in our map for management
            shadowChannels.put(shadowPath.toString(), shadowChannel);

            return shadowChannel;

        } catch (Exception e) {
            throw new RuntimeException("Failed to handle shadow file", e);
        }
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
        } else if (original instanceof LinkedHashSet) {
            return new LinkedHashSet<>(original);
        } else if (original instanceof CopyOnWriteArraySet) {
            return new CopyOnWriteArraySet<>(original);
        } else if (original instanceof HashSet) {
            return new HashSet<>(original);
        } else {
            // For unknown types, including Collections.unmodifiableSet,
            // Collections.synchronizedSet, etc., we return a new HashSet
            return new HashSet<>(original);
        }
    }

    private static <K, V> Map<K, V> cloneMap(Map<K, V> original) {
        if (original == null) {
            return null;
        }

        if (original instanceof TreeMap) {
            return new TreeMap<>(original);
        } else if (original instanceof LinkedHashMap) {
            return new LinkedHashMap<>(original);
        } else if (original instanceof ConcurrentHashMap) {
            return new ConcurrentHashMap<>(original);
        } else if (original instanceof ConcurrentSkipListMap) {
            ConcurrentSkipListMap<K, V> originalCSLM = (ConcurrentSkipListMap<K, V>) original;
            ConcurrentSkipListMap<K, V> newCSLM = new ConcurrentSkipListMap<>(originalCSLM.comparator());
            for (Map.Entry<K, V> entry : original.entrySet()) {
                newCSLM.put(entry.getKey(), entry.getValue());
            }
            return newCSLM;
        } else if (original instanceof IdentityHashMap) {
            return new IdentityHashMap<>(original);
        } else if (original instanceof WeakHashMap) {
            return new WeakHashMap<>(original);
        } else {
            System.out.println("Failure Recovery: DryRunManager.java: cloneMap: unknown type");
            // For unknown types, including Collections.unmodifiableMap,
            // Collections.synchronizedMap, etc., we use HashMap
            return new HashMap<>(original);
        }
    }

    private static <E> Queue<E> cloneQueue(Queue<E> original) {
        if (original == null) {
            return null;
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

        if (original instanceof LinkedList) {
            return new LinkedList<>(original);
        } else if (original instanceof CopyOnWriteArrayList) {
            return new CopyOnWriteArrayList<>(original);
        } else {
            return new ArrayList<>(original);
        }
    }
}

