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
    private final Queue<Pair<UUID, UUID>> allCombinations;
    private final Validator validator;

    private ExecutorService executorService;

    public StockGenerator(List<UUID> storesIds, List<UUID> productsIds) {
        this.validator = Validation.buildDefaultValidatorFactory().getValidator();
        this.faker = new DTOFaker();
        this.allCombinations = new ConcurrentLinkedQueue<>();
        initializeCombinations(storesIds, productsIds);
    }

    private void initializeCombinations(List<UUID> storesIds, List<UUID> productsIds) {
        for (UUID storeId : storesIds) {
            for (UUID productId : productsIds) {
                allCombinations.add(Pair.of(storeId, productId));
            }
        }
    }

    public void generateAndInsertStocks(int stockQty, int chunkSize, int threadPoolSize) {
        executorService = Executors.newFixedThreadPool(threadPoolSize);
        for (int i = 0; i < stockQty; i += chunkSize) {
            executorService.submit(createTask(chunkSize));
        }

        shutdownExecutor();
    }

    private Runnable createTask(int chunkSize) {
        return () -> {
            try (CqlSession connection = createSession()) {
                ShopByCategoryDAO stockDAO = new ShopByCategoryDAO(connection);
                List<ShopByCategoryDTO> shopByCategoryDTOS = new ArrayList<>();

                while (shopByCategoryDTOS.size() < chunkSize) {
                    Pair<UUID, UUID> combination = allCombinations.poll();
                    if (combination == null) break;

                    ShopByCategoryDTO stock = faker.generateShopByCategoryDTO(combination.getLeft(), combination.getRight());
                    if (!isValid(stock)) {
                        log.warn("Invalid stock data: {}", stock);
                        continue;
                    }
                    shopByCategoryDTOS.add(stock);
                }

                if (!shopByCategoryDTOS.isEmpty()) {
                    stockDAO.insertBatch(shopByCategoryDTOS);
                    log.info("Inserted {} shopByCategoryDTOs by thread {}", shopByCategoryDTOS.size(), Thread.currentThread().getName());
                }
            } catch (Exception e) {
                log.error("Error in thread {}: {}", Thread.currentThread().getName(), e.getMessage(), e);
            }
        };
    }

    private CqlSession createSession() {
        return CqlSession.builder().build();
    }

    private boolean isValid(ShopByCategoryDTO stock) {
        return validator.validate(stock).isEmpty();
    }

    private void shutdownExecutor() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(10, TimeUnit.MINUTES)) {
                    log.error("Executor did not terminate in time");
                }
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            log.error("Task interrupted during shutdown", e);
        }
        log.info("All stocks generated and inserted.");
    }
}
