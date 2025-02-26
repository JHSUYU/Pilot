import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.pilot.filesystem.ShadowFile;
import org.pilot.filesystem.ShadowFileSystem;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.pilot.PilotUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PilotUtil.class, ShadowFileSystem.class})
public class TestShadowFile {

    private File parent;
    private String child;

    @Before
    public void setUp() {
        // 创建一个临时测试目录
        parent = new File(System.getProperty("java.io.tmpdir"), "testParent");
        if (!parent.exists()) {
            parent.mkdirs();
        }
        child = "testFile.txt";
    }

    @Test
    public void testInitFileNonDryRun() {
        // 非 dry-run 模式下，方法应直接返回 new File(parent, child)
        mockStatic(PilotUtil.class);
        when(PilotUtil.isDryRun()).thenReturn(false);

        File file = ShadowFile.initFile(parent, child);
        assertNotNull("非 dry-run 模式下返回的文件不应为 null", file);
        assertEquals(new File(parent, child).getAbsolutePath(), file.getAbsolutePath());
    }

    @Test
    public void testInitFileDryRun() throws Exception {
        // dry-run 模式下，方法会调用 ShadowFileSystem 来获取 shadow 文件路径
        mockStatic(PilotUtil.class);
        when(PilotUtil.isDryRun()).thenReturn(true);

        // 模拟 ShadowFileSystem 的静态方法
        mockStatic(ShadowFileSystem.class);

        // 构造原始文件路径和预期的 shadow 路径
        Path originalPath = Paths.get(parent.getAbsolutePath(), child);
        Path shadowPath = Paths.get("/shadow", parent.getName(), child);

        File file = ShadowFile.initFile(parent, child);

        // 验证静态方法调用情况
        verifyStatic(PilotUtil.class);
        PilotUtil.isDryRun();

        verifyStatic(ShadowFileSystem.class);
        ShadowFileSystem.initializeFromOriginal();

        verifyStatic(ShadowFileSystem.class);
        ShadowFileSystem.resolveShadowFSPath(originalPath);

        assertNotNull("dry-run 模式下返回的文件不应为 null", file);
        assertEquals(shadowPath.toFile().getAbsolutePath(), file.getAbsolutePath());
    }

    @Test
    public void testInitFileIOException() throws Exception {
        // dry-run 模式下，如果 ShadowFileSystem.initializeFromOriginal() 抛出异常，方法应返回 null
        mockStatic(PilotUtil.class);
        when(PilotUtil.isDryRun()).thenReturn(true);

        mockStatic(ShadowFileSystem.class);
        // 模拟 initializeFromOriginal 抛出 IOException
        doThrow(new IOException("Test exception")).when(ShadowFileSystem.class, "initializeFromOriginal");

        File file = ShadowFile.initFile(parent, child);

        assertNull("当发生 IOException 时，返回的文件应为 null", file);
    }
}

