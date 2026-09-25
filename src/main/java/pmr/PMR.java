package pmr;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PMR {

    public static void main(String... args) {

        final int threadNum = 5;
        int i;
        final ScheduledExecutorService es = Executors.newScheduledThreadPool(threadNum);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            es.shutdown();
            try {
                es.awaitTermination(2, TimeUnit.MINUTES);
                latch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));


        for (i = 0; i < threadNum; i++) {
            es.scheduleAtFixedRate(() -> {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                DefaultExecutor executor = DefaultExecutor.builder()
                        .setExecuteStreamHandler(new PumpStreamHandler(output))
                        .get();

                try {
                    executor.execute(CommandLine.parse(String.format("whoami")));
                    log.info("Command output: {}", output.toString());
                } catch (IOException e) {
                    throw new RuntimeException("Failed to execute command", e);
                } finally {
                    if(output != null) {
                        try {
                            output.close();
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to close output stream", e);
                        }
                    }
                }
            }, 0, 200, TimeUnit.MILLISECONDS);
        }

        while(latch.getCount() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
