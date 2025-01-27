package shpp.azaika;

import com.datastax.oss.driver.api.core.CqlSession;
import jakarta.validation.Validation;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.*;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final int CHUNK_SIZE = 3000;
    private static final int THREAD_POOL_SIZE = 1;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        log.info("Starting application...");
        initializeSchema();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String productType = getProductTypeFromArgs(args);
        Properties generationProperties = loadGenerationProperties();
        DTOGenerator dtoGenerator = createDTOGenerator();

        try (ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE)) {
            List<UUID> storeIds = generateAndInsertEntities("stores", generationProperties, dtoGenerator::generateAndValidateStores, new StoreDAOFactory(), executorService);
            List<UUID> categoryIds = generateAndInsertEntities("categories", generationProperties, dtoGenerator::generateAndValidateCategories, new CategoryDAOFactory(), executorService);
            generateAndInsertProducts(dtoGenerator, generationProperties, categoryIds, executorService);
            generateAndInsertStocks(categoryIds, storeIds, generationProperties);

            String storeWithMostProducts = getStoreWithMostProductsOfType(productType);
            log.info(storeWithMostProducts);
        }

        stopWatch.stop();
        log.info("Application finished in {} ms", stopWatch.getTime());
    }

    private static void initializeSchema() throws InterruptedException {
        SchemaManager.dropAndCreateTables();
        Thread.sleep(60000);
    }

    private static String getProductTypeFromArgs(String[] args) {
        return args.length > 0 ? args[0] : "Взуття";
    }

    private static Properties loadGenerationProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));
        return properties;
    }

    private static DTOGenerator createDTOGenerator() {
        return new DTOGenerator(new DTOFaker(), new Random(), Validation.buildDefaultValidatorFactory().getValidator());
    }

    private static <T> List<UUID> generateAndInsertEntities(String entityName, Properties properties, EntityGenerator<T> generator, DAOFactory<T> daoFactory, ExecutorService executorService) throws ExecutionException, InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int quantity = Integer.parseInt(properties.getProperty(entityName + ".quantity"));
        List<T> entities = generator.generate(quantity);
        stopWatch.stop();
        log.info("Generated {} in {} ms", entityName, stopWatch.getTime());

        try (CqlSession session = CqlSession.builder().build()) {
            stopWatch.reset();
            stopWatch.start();

            Future<List<UUID>> task = executorService.submit(() -> daoFactory.create(session).insertInChunks(entities, CHUNK_SIZE));
            List<UUID> ids = task.get();

            stopWatch.stop();
            log.info("Inserted {} in DB in {} ms", entityName, stopWatch.getTime());
            return ids;
        }
    }

    private static void generateAndInsertProducts(DTOGenerator dtoGenerator, Properties properties, List<UUID> categoryIds, ExecutorService executorService) throws ExecutionException, InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int quantity = Integer.parseInt(properties.getProperty("products.quantity"));
        List<ProductDTO> products = dtoGenerator.generateAndValidateProducts(quantity, categoryIds);
        stopWatch.stop();
        log.info("Generated products in {} ms", stopWatch.getTime());

        try (CqlSession session = CqlSession.builder().build()) {
            stopWatch.reset();
            stopWatch.start();

            ProductDAO productDAO = new ProductDAO(session);
            Future<List<UUID>> task = executorService.submit(() -> productDAO.insertInChunks(products, CHUNK_SIZE));
            task.get();

            stopWatch.stop();
            log.info("Inserted products in DB in {} ms", stopWatch.getTime());
        }
    }

    private static void generateAndInsertStocks(List<UUID> categoryIds, List<UUID> storeIds, Properties properties) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int quantity = Integer.parseInt(properties.getProperty("stock.quantity"));
        StockGenerator stockGenerator = new StockGenerator(categoryIds, storeIds);
        stockGenerator.generateAndInsertStocks(quantity, CHUNK_SIZE, THREAD_POOL_SIZE);

        stopWatch.stop();
        log.info("Inserted stocks in DB in {} ms", stopWatch.getTime());
    }

    private static String getStoreWithMostProductsOfType(String productType) {
        try (CqlSession session = CqlSession.builder().build()) {
            CategoryDAO categoryDAO = new CategoryDAO(session);
            Optional<CategoryDTO> categoryOptional = categoryDAO.findByName(productType);

            if (categoryOptional.isEmpty()) {
                log.error("Category not found: {}", productType);
                return "";
            }

            UUID categoryId = categoryOptional.get().getId();
            ShopByCategoryDAO shopByCategoryDAO = new ShopByCategoryDAO(session);
            Optional<StoreDTO> storeOptional = shopByCategoryDAO.findStoreWithMostProductsByCategory(categoryId);

            if (storeOptional.isEmpty()) {
                log.error("No store found with products of type: {}", productType);
                return "";
            }

            return storeOptional.get().toString();
        }
    }

}
