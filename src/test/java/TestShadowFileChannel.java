import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pilot.PilotUtil;
import org.pilot.filesystem.ShadowFileChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(PilotUtil.class)
public class TestShadowFileChannel {
    private Path testFile;
    private Path shadowDir;

    @Mock
    private PilotUtil pilotUtil;

    @Before
    public void setUp() throws IOException {
        // 创建测试文件和目录
        System.setProperty("java.io.tmpdir", "/Users/lizhenyu/Desktop/Pilot/src/tmp");
        testFile = Files.createTempFile("test", ".txt");
        shadowDir = Files.createTempDirectory("shadow");

        // 写入一些初始数据
        String initialContent = "Hello, World! My name is the king of the system community";
        Files.write(testFile, initialContent.getBytes());

        mockStatic(PilotUtil.class);
        when(PilotUtil.isDryRun()).thenReturn(true);
    }

    @Test
    public void testWriteAndReadNewFile() throws IOException {
        // 测试新文件的写入和读取
        FileChannel channel = ShadowFileChannel.open(testFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // 写入数据
        String content = "Test content";
        ByteBuffer writeBuffer = ByteBuffer.wrap(content.getBytes());
        int bytesWritten = channel.write(writeBuffer);
        assertEquals(content.length(), bytesWritten);

        // 读取数据
        channel.position(0);
        ByteBuffer readBuffer = ByteBuffer.allocate(content.length());
        int bytesRead = channel.read(readBuffer);
        assertEquals(content.length(), bytesRead);

        readBuffer.flip();
        assertEquals(content, new String(readBuffer.array()));
    }

    @Test
    public void testAppendLog() throws IOException {
        // 测试对现有文件的写入是否正确记录到append log
        FileChannel channel = ShadowFileChannel.open(testFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // 第一次写入
        String content1 = "First write";
        ByteBuffer writeBuffer1 = ByteBuffer.wrap(content1.getBytes());
        channel.write(writeBuffer1);

        // 第二次写入
        String content2 = "Second write";
        ByteBuffer writeBuffer2 = ByteBuffer.wrap(content2.getBytes());
        channel.write(writeBuffer2);

        // 验证append log是否存在
        Path appendLogPath = ((ShadowFileChannel)channel).getShadowFileState().getAppendLogPath();
        assertTrue(Files.exists(appendLogPath));

        // 读取并验证内容
        ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        channel.position(0);
        channel.read(readBuffer);
        readBuffer.flip();

        String result = new String(readBuffer.array(), 0, readBuffer.limit());
        assertTrue(result.contains(content1));
        System.out.println(result);
        assertTrue(result.contains(content2));
    }

    @Test
    public void testPositionManagement() throws IOException {
        FileChannel channel = ShadowFileChannel.open(testFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // 测试position设置
        long newPosition = 5;
        channel.position(newPosition);
        assertEquals(newPosition, channel.position());

        // 写入数据并验证position变化
        String content = "Test";
        ByteBuffer writeBuffer = ByteBuffer.wrap(content.getBytes());
        int written = channel.write(writeBuffer);
        assertEquals(newPosition + written, channel.position());
    }

    @Test
    public void testScatterGatherOperations() throws IOException {
        FileChannel channel = ShadowFileChannel.open(testFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // 准备多个buffer进行写入
        ByteBuffer[] writeBuffers = new ByteBuffer[2];
        writeBuffers[0] = ByteBuffer.wrap("First ".getBytes());
        writeBuffers[1] = ByteBuffer.wrap("Second".getBytes());

        // 写入数据
        long totalWritten = channel.write(writeBuffers, 0, 2);
        assertEquals(12, totalWritten); // "First Second" = 12 bytes

        // 读取数据
        channel.position(0);
        ByteBuffer[] readBuffers = new ByteBuffer[2];
        readBuffers[0] = ByteBuffer.allocate(6);
        readBuffers[1] = ByteBuffer.allocate(6);
        long totalRead = channel.read(readBuffers, 0, 2);

        assertEquals(totalWritten, totalRead);
    }

    @Test
    public void testConcurrentAccess() throws IOException {
        // 测试同一文件的多个Channel访问
        FileChannel channel1 = ShadowFileChannel.open(testFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel channel2 = ShadowFileChannel.open(testFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // channel1写入数据
        String content = "Channel 1 write";
        ByteBuffer writeBuffer = ByteBuffer.wrap(content.getBytes());
        channel1.write(writeBuffer);

        // channel2读取数据
        ByteBuffer readBuffer = ByteBuffer.allocate(content.length());
        channel2.position(0);
        channel2.read(readBuffer);
        readBuffer.flip();

        assertEquals(content, new String(readBuffer.array()));
    }
}