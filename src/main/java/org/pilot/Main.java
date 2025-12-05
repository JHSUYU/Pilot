package org.pilot;

import io.opentelemetry.context.Context;

import java.lang.reflect.Field;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        Context context = Context.current();
        Context pilotContext = PilotUtil.getPilotContextInternal(context,1);
        pilotContext.makeCurrent();

        testObject to=new testObject();
        to.info();

        Field contextField = findPilotContextField(to.getClass());

        if (contextField != null) {
            contextField.setAccessible(true);

            // 获取并设置当前 Context
            Context ctx= Context.current();
            try{
                contextField.set(to, ctx);
            }catch(Exception e){
                System.out.println("fail to set PilotContext field");
                e.printStackTrace();
            }
        }
        to.info();

    }



    private static Field findPilotContextField(Class<?> clazz) {
        Class<?> current = clazz;
        while (current != null && !current.equals(Object.class)) {
            try {
                return current.getDeclaredField("PilotContext");
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    public static void startThread(Thread t) {
        if (!PilotUtil.isDryRun()) {
            t.start();
            return;
        }

        System.out.println("Starting thread in dry run mode: " + t.getName());

        try {
            Field targetField = Thread.class.getDeclaredField("target");
            targetField.setAccessible(true);
            Runnable originalTarget = (Runnable) targetField.get(t);

            if (originalTarget != null) {
                // target 不为 null：包装 target
                Context ctx = Context.current();
                Runnable wrappedTarget = ctx.wrap(() -> {
                    try {
                        System.out.println("Thread {} is registering to ZK"+t.getName()+PilotUtil.isDryRun());
                        originalTarget.run();
                    } finally {
                        System.out.println("Thread " + t.getName() + " finished execution");
                    }
                });

                targetField.set(t, wrappedTarget);

            } else {

            }

        } catch (Exception e) {
            System.out.println("fail");
        }

        t.start();
    }

}