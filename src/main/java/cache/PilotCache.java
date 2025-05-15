package cache;

import java.util.HashMap;
import java.util.Map;

public class PilotCache {
//    private static Cloner cloner = new Cloner();
    private static Map<String, Object> cache = new HashMap<>();

    public static <T> void set(String key, T value) {
        cache.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(String key) {
        Object value = cache.get(key);
        if (value == null) {
            return null;
        }
        return (T) value;
    }

    public static boolean contains(String key){
        return cache.containsKey(key);
    }

    public static void remove(String key) {
        cache.remove(key);
    }

    public static void clear() {
        cache.clear();
    }
}
