package org.pilot.runtime;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.UUID;

public class PilotRuntime {

    public static boolean enablePilot = true;

    private static ModuleInfo singlePilotModuleInfo;

    // 模拟运行时初始化，直接接收 Pilot 模块信息
    public static void init(ModuleInfo moduleInfo) {
        singlePilotModuleInfo = moduleInfo;
        System.out.println("[PilotRuntime] Autopilot PilotRuntime initialized with module: " + moduleInfo.moduleJarUrl);
    }

    public static void startPilotExecution(Object... otherArgs){
        System.out.println("\n--- Autopilot PilotRuntime: Pilot Execution Triggered ---");

        if (singlePilotModuleInfo == null) {
            System.err.println("[PilotRuntime Error] Autopilot PilotRuntime not initialized with pilot module info.");
            return;
        }

        // 确保 otherArgs 数组不为 null
        if (otherArgs == null) {
            otherArgs = new Object[0]; // Treat null varargs as empty array
        }

        CustomPilotClassLoader pilotClassLoader = null;
        Object pilotEntryInstance = null;
        String pilotExecutionId = "exec-" + UUID.randomUUID().toString().substring(0, 8);

        try {
            // 1. 按需创建并加载 Pilot 模块 ClassLoader
            pilotClassLoader = new CustomPilotClassLoader(new URL[]{singlePilotModuleInfo.moduleJarUrl},
                    // 使用加载 AutopilotRuntime 的 ClassLoader 作为父加载器
                    PilotRuntime.class.getClassLoader());

            // 2. 使用加载器获取 Pilot 入口类 (这个类在 Pilot 模块 JAR 里)
            Class<?> pilotEntryClass = pilotClassLoader.loadClass(singlePilotModuleInfo.pilotEntryClassName);

            // 3. 实例化 Pilot 入口对象
            pilotEntryInstance = pilotEntryClass.newInstance(); // 需要无参构造器

            // 4. 准备并查找 Pilot 入口方法
            //  Pilot 入口方法现在应该接收 Map<String, Object> 和 Object (或 NodeProbe)
            Method executePilotMethod = findPilotExecuteMethod(pilotEntryClass, otherArgs);
            if (executePilotMethod == null) {
                System.err.println("[PilotRuntime Error] Pilot entry class " + singlePilotModuleInfo.pilotEntryClassName + " missing required execute method with signature executePilot(Map<String, Object>, Object).");
                return;
            }

            // 在新线程中运行 Pilot（模拟影子线程）
            CustomPilotClassLoader finalPilotClassLoader = pilotClassLoader;
            Object finalPilotEntryInstance = pilotEntryInstance;
            Object[] finalOtherArgs = otherArgs;


            String currentPilotExecutionId = "exec-" + UUID.randomUUID().toString().substring(0, 8);
            System.out.println("Pilot Thread received " + (finalOtherArgs != null ? finalOtherArgs.length : 0) + " arguments.");
            // 可以打印收到的参数类型以便调试
            // for(int i = 0; i < finalOtherArgs.length; i++) {
            //      System.out.println("  Arg " + i + ": " + (finalOtherArgs[i] != null ? finalOtherArgs[i].getClass().getName() : "null"));
            // }


            try {
                executePilotMethod.invoke(finalPilotEntryInstance, (Object[]) finalOtherArgs);
                System.out.println("Pilot Thread Finished Successfully.");
            } catch (IllegalAccessException e) {
                System.err.println("Pilot Thread Reflection Access Error: " + e.getMessage());
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                System.err.println("Pilot Thread Reflection Argument Error: " + e.getMessage());
                System.err.println("Expected parameters for " + executePilotMethod.getName() + ": " + java.util.Arrays.toString(executePilotMethod.getParameterTypes()));
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                // 捕获目标方法（Pilot代码）内部抛出的异常
                System.err.println("Pilot Thread Invocation Error (Exception in Pilot code): " + e.getTargetException().getMessage());
                e.getTargetException().printStackTrace();
            } catch (Throwable e) { // 捕获 Throwable 以防 Error 导致线程死亡
                System.err.println("Pilot Thread Unexpected Error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("Pilot Thread Context Cleared.");
            }

            System.out.println("startPilotExecution method finished. Pilot execution is running in background (simulated).");

        } catch (Exception e) {
            System.err.println("[PilotRuntime Error] Exception setting up or starting Pilot execution: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private static Method findPilotExecuteMethod(Class<?> pilotEntryClass, Object[] otherArgs) {
        Class<?>[] argTypes = new Class<?>[otherArgs != null ? otherArgs.length : 0];
        if (otherArgs != null) {
            for (int i = 0; i < otherArgs.length; i++) {
                if (otherArgs[i] != null) {
                    argTypes[i] = otherArgs[i].getClass();
                } else {
                    argTypes[i] = null; // null 参数可以匹配任何非基本类型
                }
            }
        }

        for (Method method : pilotEntryClass.getMethods()) {
            if (method.getName().equals("execute$pilot") &&
                    method.getParameterCount() == argTypes.length) {

                boolean typesMatch = true;
                Class<?>[] methodParams = method.getParameterTypes(); // 获取目标方法的参数类型列表

                // 逐个检查参数类型是否兼容
                for (int i = 0; i < argTypes.length; i++) {
                    if (argTypes[i] != null) {
                        if (!methodParams[i].isAssignableFrom(argTypes[i])) {
                            typesMatch = false;
                            break;
                        }
                    } else {
                        // 如果传入的参数是 null，则目标方法的参数类型必须是非基本类型
                        if (methodParams[i] != null && methodParams[i].isPrimitive()) {
                            typesMatch = false;
                            break;
                        }
                    }
                }

                // 如果所有参数类型都兼容
                if (typesMatch) {
                    System.out.println("[PilotRuntime] Found compatible method: " + method);
                    return method; // 找到第一个匹配的方法就返回
                }
            }
        }

        // 如果遍历完所有方法都没有找到匹配的
        System.err.println("[PilotRuntime] No compatible method named 'executePilot' found with " + argTypes.length + " parameters.");
        return null; // 没有找到匹配的方法
    }
}
