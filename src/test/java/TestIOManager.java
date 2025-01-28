

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pilot.State;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;


import org.junit.Test;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import static org.junit.Assert.*;

public class TestIOManager {

    @Test
    public void testFileChannelShallowCopy() throws Exception {
        // 准备测试文件和路径
        Path testFilePath = Paths.get("test_channel.txt");
        Path shadowFilePath = Paths.get("shadow", "test_channel.txt");

        try {
            // 创建并写入测试文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(testFilePath.toFile()))) {
                writer.write("test content for channel");
            }

            // 创建原始FileChannel并测试
            FileChannel originalChannel = FileChannel.open(testFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            FileChannel shadowChannel = State.shallowCopy(originalChannel, null, false);

            // 验证
            assertNotNull("Shadow channel should not be null", shadowChannel);
            assertNotEquals("Shadow channel should be different from original", originalChannel, shadowChannel);
            assertTrue("Shadow file should exist", Files.exists(shadowFilePath));
            assertEquals("File sizes should match", originalChannel.size(), shadowChannel.size());

            // 清理
            originalChannel.close();
            shadowChannel.close();
            Files.deleteIfExists(testFilePath);
            Files.deleteIfExists(shadowFilePath);
            Files.deleteIfExists(shadowFilePath.getParent());

        } finally {
            State.IOManager.closeAllShadowChannels();
        }
    }

    @Test
    public void testFileOutputStreamShallowCopy() throws Exception {
        // 准备测试文件和路径
        Path testFilePath = Paths.get("/Users/lizhenyu/Desktop/Pilot/src/test/java/test_channel.txt");
        Path shadowFilePath = Paths.get("/Users/lizhenyu/Desktop/Pilot/src/test/java/shadow/test_channel.txt");

        try {
            // 创建并写入测试文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(testFilePath.toFile()))) {
                writer.write("test content for stream");
            }

            // 创建原始FileOutputStream并测试
            FileOutputStream originalStream = new FileOutputStream(testFilePath.toFile());
            FileOutputStream shadowStream = State.shallowCopy(originalStream, null, false);

            // 验证
            assertNotNull("Shadow stream should not be null", shadowStream);
            assertNotEquals("Shadow stream should be different from original", originalStream, shadowStream);
            assertTrue("Shadow file should exist", Files.exists(shadowFilePath));

            // 测试写入
            String testData = "test output";
            shadowStream.write(testData.getBytes());
            shadowStream.flush();

            // 验证写入的数据
            String shadowContent = new String(Files.readAllBytes(shadowFilePath));
            assertTrue("Shadow file should contain written data", shadowContent.contains(testData));

            // 清理
            originalStream.close();
            shadowStream.close();
            Files.deleteIfExists(testFilePath);
            Files.deleteIfExists(shadowFilePath);
            Files.deleteIfExists(shadowFilePath.getParent());

        } finally {
            State.IOManager.closeAllShadowChannels();
        }
    }
}
