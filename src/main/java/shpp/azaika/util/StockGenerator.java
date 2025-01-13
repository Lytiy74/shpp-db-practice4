package shpp.azaika.util;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.StockDAO;
import shpp.azaika.dto.StockDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StockGenerator {
    private static final Logger log = LoggerFactory.getLogger(StockGenerator.class);
    private final DTOFaker faker;
    private final Queue<Pair<Short, Short>> allCombinations = new ConcurrentLinkedQueue<>();
    private final StockDAO stockDAO;

    public StockGenerator(List<Short> storesIds, List<Short> productsIds, StockDAO stockDAO) {
        this.faker = new DTOFaker();
        this.stockDAO = stockDAO;
        for (Short storeId : storesIds) {
            for (Short productId : productsIds) {
                allCombinations.add(Pair.of(storeId, productId));
            }
        }
    }

    public void generateAndInsertStocks(int stockQty, int chunkSize, int threadCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        Runnable task = () -> {
            List<StockDTO> stocks = new ArrayList<>();
            while (stocks.size() < chunkSize) {
                Pair<Short, Short> combination = allCombinations.poll();
                if (combination == null) break;
                StockDTO stock = faker.generateStockDTO(combination.getLeft(), combination.getRight());
                stocks.add(stock);
            }
            if (!stocks.isEmpty()) {
                stockDAO.insertBatch(stocks);
                log.info("Inserted {} stocks by thread {}", stocks.size(), Thread.currentThread().getName());
            }
        };

        for (int i = 0; i < stockQty; i += chunkSize) {
            executorService.submit(task);
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Task interrupted", e);
        }

        log.info("All stocks generated and inserted.");
    }
}
