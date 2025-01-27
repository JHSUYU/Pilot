package org.pilot;

public class WrapContext <T> {
    public static int id = 0;

    public T value;

    public T getValue(){
        System.out.println("FL, getValue() called with id: " + id + " value: " + value);
        id++;
        return value;
    }

    public WrapContext(T value) {
        this.value = value;
    }
}
