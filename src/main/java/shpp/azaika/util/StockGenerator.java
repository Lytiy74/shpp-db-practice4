package shpp.azaika.util;

import com.datastax.oss.driver.api.core.CqlSession;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.ShopByCategoryDAO;
import shpp.azaika.dto.ShopByCategoryDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StockGenerator {
    private static final Logger log = LoggerFactory.getLogger(StockGenerator.class);
    private final DTOFaker faker;
    private final Queue<Pair<UUID, UUID>> allCombinations = new ConcurrentLinkedQueue<>();
    private final Validator validator;

    public StockGenerator(List<UUID> storesIds, List<UUID> productsIds) {
        this.validator = Validation.buildDefaultValidatorFactory().getValidator();
        this.faker = new DTOFaker();
        for (UUID storeId : storesIds) {
            for (UUID productId : productsIds) {
                allCombinations.add(Pair.of(storeId, productId));
            }
        }
    }

    public void generateAndInsertStocks(int stockQty, int chunkSize, int threadCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        Runnable task = () -> {
            try (CqlSession connection = CqlSession.builder().build()) {
                ShopByCategoryDAO stockDAO = new ShopByCategoryDAO(connection);
                List<ShopByCategoryDTO> stocks = new ArrayList<>();

                while (stocks.size() < chunkSize) {
                    Pair<UUID, UUID> combination = allCombinations.poll();
                    if (combination == null) break;

                    ShopByCategoryDTO stock = faker.generateShopByCategoryDTO(combination.getLeft(), combination.getRight());
                    if(!validator.validate(stock).isEmpty()) continue;
                    stocks.add(stock);
                }

                if (!stocks.isEmpty()) {
                    stockDAO.insertBatch(stocks);
                    log.info("Inserted {} stocks by thread {}", stocks.size(), Thread.currentThread().getName());
                }
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
