import org.pilot.filesystem.ShadowFile;

import java.io.File;

public class ShadowFileInitTest {
    public static void main(String[] args) {
        // 准备测试用的父目录和子文件名
        File parent = new File("testDir");
        String child = "testFile.txt";

        // 如果测试目录不存在，则创建它
        if (!parent.exists()) {
            if (parent.mkdirs()) {
                System.out.println("成功创建测试目录: " + parent.getAbsolutePath());
            } else {
                System.out.println("创建测试目录失败: " + parent.getAbsolutePath());
            }
        }

        // 调用 initFile 方法
        File initializedFile = ShadowFile.initFile(parent, child);

        // 输出测试结果
        if (initializedFile != null) {
            System.out.println("Initialized file path: " + initializedFile.getAbsolutePath());
        } else {
            System.out.println("initFile 方法返回 null，初始化失败。");
        }
    }
}
