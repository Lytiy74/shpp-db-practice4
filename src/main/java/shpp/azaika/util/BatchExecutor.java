package shpp.azaika.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.Dao;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BatchExecutor<T> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BatchExecutor.class);

    private final BlockingQueue<T> inputQueue;
    private final BlockingQueue<Long> outputQueue;

    private final Dao<T> dao;

    private final int batchSize;

    public BatchExecutor(BlockingQueue<T> inputQueue, BlockingQueue<Long> outputQueue, Dao<T> dao, int batchSize) {
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.dao = dao;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            int processedBatchSize = 0;
            while (true) {
                T item = inputQueue.poll(5, TimeUnit.SECONDS);
                dao.addToBatch(item);
                processedBatchSize++;

                if (processedBatchSize >= batchSize) {
                    processBatch();
                    processedBatchSize = 0;
                }
            }
        } catch (InterruptedException | SQLException e) {
            log.error("BatchExecutor interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private void processBatch() throws SQLException, InterruptedException {
        List<Long> ids = dao.executeBatch();
        for (Long id : ids) {
            outputQueue.put(id);
        }
    }
}