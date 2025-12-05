package Test;

import io.opentelemetry.context.Context;

public class TestThread extends Thread{
    public Context PilotContext;

    public void run(){
        System.out.println("Thread is running");
    }
}
