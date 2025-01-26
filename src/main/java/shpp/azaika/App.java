package shpp.azaika;

import com.datastax.oss.driver.api.core.CqlSession;
import jakarta.validation.Validation;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.CategoryDAO;
import shpp.azaika.dao.ProductDAO;
import shpp.azaika.dao.ShopByCategoryDAO;
import shpp.azaika.dao.StoreDAO;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.DTOFaker;
import shpp.azaika.util.DTOGenerator;
import shpp.azaika.util.StockGenerator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final int CHUNK_SIZE = 3000;

    public static void main(String[] args) throws IOException, SQLException, ExecutionException, InterruptedException {
        log.info("Starting application...");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String productType = getProductTypeFromArgs(args);
        Properties generationProperties = loadGenerationProperties();

        DTOGenerator dtoGenerator = new DTOGenerator(new DTOFaker(), new Random(), Validation.buildDefaultValidatorFactory().getValidator());


        ExecutorService executorService = Executors.newFixedThreadPool(4);

        List<UUID> storeIds = generateAndInsertStores(dtoGenerator, generationProperties, executorService);
        List<UUID> categoriesIds = generateAndInsertCategories(dtoGenerator, generationProperties, executorService);
        List<UUID> productIds = generateAndInsertProducts(dtoGenerator, generationProperties, categoriesIds, executorService);
        generateAndInsertToShopByCategory(categoriesIds, storeIds, generationProperties);


    }

    private static void generateAndInsertToShopByCategory(List<UUID> categoriesIds, List<UUID> storeIds, Properties generationProperties) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        StockGenerator stockGenerator = new StockGenerator(categoriesIds, storeIds);
        stockGenerator.generateAndInsertStocks(Integer.parseInt(generationProperties.getProperty("stock.quantity")), CHUNK_SIZE, 3);
        stopWatch.stop();
        log.info("Inserted stocks in DB in {} ms", stopWatch.getDuration().toMillis());
    }

    private static String getProductTypeFromArgs(String[] args) {
        return args.length > 0 ? args[0] : "Взуття";
    }

    private static Properties loadGenerationProperties() throws IOException {
        Properties generationProperties = new Properties();
        generationProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));
        return generationProperties;
    }

    private static List<UUID> generateAndInsertStores(DTOGenerator dtoGenerator, Properties generationProperties, ExecutorService executorService) throws InterruptedException, ExecutionException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<StoreDTO> storeDTOS = dtoGenerator.generateAndValidateStores(Integer.parseInt(generationProperties.getProperty("stores.quantity")));
        stopWatch.stop();
        log.info("Generated stores in {} ms", stopWatch.getDuration().toMillis());

        List<UUID> ids;
        try (CqlSession connection = CqlSession.builder().build()) {
            StoreDAO storeDAO = new StoreDAO(connection);
            stopWatch.reset();
            stopWatch.start();
            Future<List<UUID>> storeInsertTask = executorService.submit(() -> storeDAO.insertInChunks(storeDTOS, CHUNK_SIZE));
            ids = storeInsertTask.get();
            stopWatch.stop();
            log.info("Inserted stores in DB in {} ms", stopWatch.getDuration().toMillis());
        }

        return ids;
    }

    private static List<UUID> generateAndInsertCategories(DTOGenerator dtoGenerator, Properties generationProperties, ExecutorService executorService) throws InterruptedException, ExecutionException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<CategoryDTO> categoryDTOS = dtoGenerator.generateAndValidateCategories(Integer.parseInt(generationProperties.getProperty("categories.quantity")));
        stopWatch.stop();
        log.info("Generated categories in {} ms", stopWatch.getDuration().toMillis());

        List<UUID> ids;
        try (CqlSession connection = CqlSession.builder().build()) {
            CategoryDAO categoryDAO = new CategoryDAO(connection);
            stopWatch.reset();
            stopWatch.start();
            Future<List<UUID>> categoryInsertTask = executorService.submit(() -> categoryDAO.insertInChunks(categoryDTOS, CHUNK_SIZE));
            ids = categoryInsertTask.get();
            stopWatch.stop();
            log.info("Inserted categories in DB in {} ms", stopWatch.getDuration().toMillis());
        }

        return ids;
    }

    private static List<UUID> generateAndInsertProducts(DTOGenerator dtoGenerator, Properties generationProperties, List<UUID> categoryIds, ExecutorService executorService) throws InterruptedException, ExecutionException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<ProductDTO> productDTOS = dtoGenerator.generateAndValidateProducts(Integer.parseInt(generationProperties.getProperty("products.quantity")), categoryIds);
        stopWatch.stop();
        log.info("Generated products in {} ms", stopWatch.getDuration().toMillis());

        List<UUID> ids;
        try (CqlSession connection = CqlSession.builder().build()) {
            ProductDAO productDAO = new ProductDAO(connection);
            stopWatch.reset();
            stopWatch.start();
            Future<List<UUID>> productInsertTask = executorService.submit(() -> productDAO.insertInChunks(productDTOS, CHUNK_SIZE));
            ids = productInsertTask.get();
            stopWatch.stop();
            log.info("Inserted products in DB in {} ms", stopWatch.getDuration().toMillis());
        }

        return ids;
    }


    private static String getStoreWithMostProductsOfType(String productType, CategoryDAO categoryDAO, ShopByCategoryDAO shopByCategoryDAO) throws SQLException {
        Optional<CategoryDTO> categoryOptional = categoryDAO.findByName(productType);
        if (categoryOptional.isEmpty()) {
            log.error("Category not found.");
            return "";
        }
        CategoryDTO category = categoryOptional.get();

        Optional<StoreDTO> storeOptional = shopByCategoryDAO.findStoreWithMostProductsByCategory(category.getId());
        if (storeOptional.isEmpty()) {
            log.error("No store found with products of type {}.", productType);
            return "";
        }
        StoreDTO storeWithMostProducts = storeOptional.get();
        return storeWithMostProducts.toString();
    }

}
