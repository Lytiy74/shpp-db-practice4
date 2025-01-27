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

        storesIds.parallelStream().forEach(storeId ->
                productsIds.forEach(productId ->
                        allCombinations.add(Pair.of(storeId, productId))
                )
        );
    }

    public void generateAndInsertStocks(int stockQty, int chunkSize, int threadCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < stockQty; i += chunkSize) {
            executorService.submit(getTask(chunkSize));
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

    private Runnable getTask(int chunkSize) {
        return () -> {
            try (CqlSession connection = CqlSession.builder().build()) {
                ShopByCategoryDAO stockDAO = new ShopByCategoryDAO(connection);
                List<ShopByCategoryDTO> shopByCategoryDTOS = new ArrayList<>();

                while (shopByCategoryDTOS.size() < chunkSize) {
                    Pair<UUID, UUID> combination = allCombinations.poll();
                    if (combination == null) break;

                    ShopByCategoryDTO stock = faker.generateShopByCategoryDTO(combination.getLeft(), combination.getRight());
                    if (!validator.validate(stock).isEmpty()) {
                        log.warn("Invalid stock data: {}", stock);
                        continue;
                    }
                    shopByCategoryDTOS.add(stock);
                }

                if (!shopByCategoryDTOS.isEmpty()) {
                    stockDAO.insertBatch(shopByCategoryDTOS);
                    log.info("Inserted {} shopByCategoryDTOS by thread {}", shopByCategoryDTOS.size(), Thread.currentThread().getName());
                }
            } catch (Exception e) {
                log.error("Error in thread {}: {}", Thread.currentThread().getName(), e.getMessage(), e);
            }
        };
    }
}
