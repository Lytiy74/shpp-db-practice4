package shpp.azaika;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.CategoryDAO;
import shpp.azaika.dao.ProductDAO;
import shpp.azaika.dao.StockDAO;
import shpp.azaika.dao.StoreDAO;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.DTOGenerator;
import shpp.azaika.util.StockGenerator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final int CHUNK_SIZE = 3000;
    private static List<Short> ids;

    public static void main(String[] args) throws IOException, SQLException, ExecutionException, InterruptedException {
        log.info("Starting application...");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String productType = getProductTypeFromArgs(args);
        Properties generationProperties = loadGenerationProperties();

        DataSource dataSource = DataSource.getInstance();
        DTOGenerator dtoGenerator = new DTOGenerator();

        dropAndCreateTables(dataSource.getConnection());

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<Short> storeIds = generateAndInsertStores(dtoGenerator, generationProperties, executorService, dataSource);
        List<Short> categoryIds = generateAndInsertCategories(dtoGenerator, generationProperties, executorService, dataSource);
        List<Short> productIds = generateAndInsertProducts(dtoGenerator, generationProperties, categoryIds, executorService, dataSource);

        generateAndInsertStockData(storeIds, productIds, generationProperties, dataSource);

        String storeWithMostProductsOfType = getStoreWithMostProductsOfType(productType, new CategoryDAO(dataSource.getConnection()), new StockDAO(dataSource.getConnection()));
        log.info("Store with most products of type {}: {}", productType, storeWithMostProductsOfType);

        executorService.shutdown();
        log.info("Application finished in {} ms", stopWatch.getDuration().toMillis());
    }

    private static String getProductTypeFromArgs(String[] args) {
        return args.length > 0 ? args[0] : "Взуття";
    }

    private static Properties loadGenerationProperties() throws IOException {
        Properties generationProperties = new Properties();
        generationProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));
        return generationProperties;
    }

    private static List<Short> generateAndInsertStores(DTOGenerator dtoGenerator, Properties generationProperties, ExecutorService executorService, DataSource dataSource) throws InterruptedException, ExecutionException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<StoreDTO> storeDTOS = dtoGenerator.generateAndValidateStores(Integer.parseInt(generationProperties.getProperty("stores.quantity")));
        stopWatch.stop();
        log.info("Generated stores in {} ms", stopWatch.getDuration().toMillis());

        List<Short> ids;
        try (Connection connection = dataSource.getConnection()) {
            StoreDAO storeDAO = new StoreDAO(connection);
            stopWatch.reset();
            stopWatch.start();
            Future<List<Short>> storeInsertTask = executorService.submit(() -> storeDAO.insertInChunks(storeDTOS, CHUNK_SIZE));
            ids = storeInsertTask.get();
            stopWatch.stop();
            log.info("Inserted stores in DB in {} ms", stopWatch.getDuration().toMillis());
        }

        return ids;
    }

    private static List<Short> generateAndInsertCategories(DTOGenerator dtoGenerator, Properties generationProperties, ExecutorService executorService, DataSource dataSource) throws InterruptedException, ExecutionException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<CategoryDTO> categoryDTOS = dtoGenerator.generateAndValidateCategories(Integer.parseInt(generationProperties.getProperty("categories.quantity")));
        stopWatch.stop();
        log.info("Generated categories in {} ms", stopWatch.getDuration().toMillis());

        List<Short> ids;
        try (Connection connection = dataSource.getConnection()) {
            CategoryDAO categoryDAO = new CategoryDAO(connection);
            stopWatch.reset();
            stopWatch.start();
            Future<List<Short>> categoryInsertTask = executorService.submit(() -> categoryDAO.insertInChunks(categoryDTOS, CHUNK_SIZE));
            ids = categoryInsertTask.get();
            stopWatch.stop();
            log.info("Inserted categories in DB in {} ms", stopWatch.getDuration().toMillis());
        }

        return ids;
    }

    private static List<Short> generateAndInsertProducts(DTOGenerator dtoGenerator, Properties generationProperties, List<Short> categoryIds, ExecutorService executorService, DataSource dataSource) throws InterruptedException, ExecutionException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<ProductDTO> productDTOS = dtoGenerator.generateAndValidateProducts(Integer.parseInt(generationProperties.getProperty("products.quantity")), categoryIds);
        stopWatch.stop();
        log.info("Generated products in {} ms", stopWatch.getDuration().toMillis());

        List<Short> ids;
        try (Connection connection = dataSource.getConnection()) {
            ProductDAO productDAO = new ProductDAO(connection);
            stopWatch.reset();
            stopWatch.start();
            Future<List<Short>> productInsertTask = executorService.submit(() -> productDAO.insertInChunks(productDTOS, CHUNK_SIZE));
            ids = productInsertTask.get();
            stopWatch.stop();
            log.info("Inserted products in DB in {} ms", stopWatch.getDuration().toMillis());
        }

        return ids;
    }

    private static void generateAndInsertStockData(List<Short> storeIds, List<Short> productIds, Properties generationProperties, DataSource dataSource) throws SQLException {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            StockGenerator stockGenerator = new StockGenerator(storeIds, productIds, dataSource);
            stockGenerator.generateAndInsertStocks(Integer.parseInt(generationProperties.getProperty("stock.quantity")), CHUNK_SIZE, 3);
            stopWatch.stop();
            log.info("Inserted stocks in DB in {} ms", stopWatch.getDuration().toMillis());
    }

    private static String getStoreWithMostProductsOfType(String productType, CategoryDAO categoryDAO, StockDAO stockDAO) throws SQLException {
        Optional<CategoryDTO> categoryOptional = categoryDAO.findByName(productType);
        if (categoryOptional.isEmpty()) {
            log.error("Category not found.");
            return "";
        }
        CategoryDTO category = categoryOptional.get();

        Optional<StoreDTO> storeOptional = stockDAO.findStoreWithMostProductsByCategory(category.getId());
        if (storeOptional.isEmpty()) {
            log.error("No store found with products of type {}.", productType);
            return "";
        }
        StoreDTO storeWithMostProducts = storeOptional.get();
        return storeWithMostProducts.toString();
    }

    public static void dropAndCreateTables(Connection connection) {
        String sql = """
                DROP TABLE IF EXISTS stocks;
                DROP TABLE IF EXISTS products;
                DROP TABLE IF EXISTS categories;
                DROP TABLE IF EXISTS stores;
                CREATE TABLE categories (id smallserial PRIMARY KEY, name varchar(255) NOT NULL);
                CREATE TABLE products (id smallserial PRIMARY KEY, name varchar(255) NOT NULL, category_id smallint NOT NULL REFERENCES categories, price numeric NOT NULL);
                CREATE TABLE stores (id smallserial PRIMARY KEY, address varchar(255) NOT NULL);
                CREATE TABLE stocks (shop_id smallint NOT NULL REFERENCES stores, product_id smallint NOT NULL REFERENCES products, quantity integer NOT NULL, PRIMARY KEY (shop_id, product_id));
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Error creating tables", e);
        }
    }
}
