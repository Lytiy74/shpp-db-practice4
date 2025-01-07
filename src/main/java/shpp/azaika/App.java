package shpp.azaika;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.*;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.DTOGenerator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) throws IOException {
        log.info("Starting application...");
        String productType = args.length > 0 ? args[0] : "Взуття";

        Properties generationProperties = new Properties();
        generationProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));
        int storesQuantity = Integer.parseInt(generationProperties.getProperty("stores.quantity"));
        int categoriesQuantity = Integer.parseInt(generationProperties.getProperty("categories.quantity"));
        int productsQuantity = Integer.parseInt(generationProperties.getProperty("products.quantity"));
        int stocksQuantity = Integer.parseInt(generationProperties.getProperty("stock.quantity"));

        Properties dbProperties = new Properties();
        dbProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties"));

        try (Connection connection = DriverManager.getConnection(
                dbProperties.getProperty("db.url"), dbProperties.getProperty("db.user"), dbProperties.getProperty("db.password"));
             ExecutorService executorService = Executors.newFixedThreadPool(4)) { // обмеження на 4 потоки
            StopWatch totalWatch = StopWatch.createStarted();
            DAOContainer daoContainer = initializeDAOs(connection);
            log.info("Generating {} stores, {} categories, {} products and {} stocks...", storesQuantity, categoriesQuantity, productsQuantity, stocksQuantity);
            DTOGenerator generator = new DTOGenerator();
            // Використовуємо Callable для паралельного виконання завдань
            Future<List<Long>> categoryTask = executorService.submit(() -> {
                StopWatch stopWatch = StopWatch.createStarted();
                log.info("Generating categories...");
                Set<CategoryDTO> categoryDTOS = generator.generateCategories(categoriesQuantity);
                for (CategoryDTO categoryDTO : categoryDTOS) {
                    daoContainer.categoryDAO.addToBatch(categoryDTO);
                }
                log.info("Categories generated in {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
                StopWatch batching = StopWatch.createStarted();
                List<Long> ids = daoContainer.categoryDAO.executeBatch();
                log.info("Categories batched in {} ms", batching.getTime(TimeUnit.MILLISECONDS));
                return ids;
            });

            Future<List<Long>> storeTask = executorService.submit(() -> {
                StopWatch stopWatch = StopWatch.createStarted();
                log.info("Generating stores...");
                Set<StoreDTO> storeDTOS = generator.generateStores(storesQuantity);
                for (StoreDTO storeDTO : storeDTOS) {
                    daoContainer.storeDAO.addToBatch(storeDTO);
                }
                log.info("Stores generated in {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
                StopWatch batching = StopWatch.createStarted();
                List<Long> ids = daoContainer.storeDAO.executeBatch();
                log.info("Stores batched in {} ms", batching.getTime(TimeUnit.MILLISECONDS));
                return ids;
            });

            // Генеруємо категорії перед подальшою обробкою
            List<Long> categoriesIds = categoryTask.get(); // Очікуємо завершення завдання
            Future<List<Long>> productTask = executorService.submit(() -> {
                StopWatch stopWatch = StopWatch.createStarted();
                log.info("Generating products...");
                Set<ProductDTO> productDTOS = generator.generateProducts(productsQuantity, categoriesIds);
                for (ProductDTO productDTO : productDTOS) {
                    daoContainer.productDAO.addToBatch(productDTO);
                }
                log.info("Products generated in {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
                StopWatch batching = StopWatch.createStarted();
                List<Long> ids = daoContainer.productDAO.executeBatch();
                log.info("Products batched in {} ms", batching.getTime(TimeUnit.MILLISECONDS));
                return ids;
            });

            List<Long> storesIds = storeTask.get(); // Отримуємо результат із завдання
            List<Long> productsIds = productTask.get(); // Отримуємо продукти перед генерацією складів

            Future<Void> stockTask = executorService.submit(() -> {
                StopWatch stopWatch = StopWatch.createStarted();
                log.info("Generating stocks...");
                Set<StockDTO> stockDTOS = generator.generateStocks(stocksQuantity, storesIds, productsIds);
                for (StockDTO stockDTO : stockDTOS) {
                    daoContainer.stockDAO.addToBatch(stockDTO);
                }
                log.info("Stocks generated in {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
                StopWatch batching = StopWatch.createStarted();
                daoContainer.stockDAO.executeBatch();
                log.info("Stocks batched in {} ms", batching.getTime(TimeUnit.MILLISECONDS));
                return null;
            });

            stockTask.get();

            log.info("Querying store with most products of type: {}", productType);
            String storeAddress = getStoreWithMostProductsOfType(daoContainer, productType);
            log.info("Store with most products of type {}: {}", productType, storeAddress);
            log.info("Total time: {} ms", totalWatch.getTime(TimeUnit.MILLISECONDS));

        } catch (SQLException e) {
            log.error("Database error occurred", e);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred during multi-threaded execution", e);
        }
    }

    private static String getStoreWithMostProductsOfType(DAOContainer daoContainer, String productType) throws SQLException {

        Optional<CategoryDTO> categoryOptional = daoContainer.categoryDAO.findByName(productType);
        if (categoryOptional.isEmpty()) {
            log.error("Category not found.");
            return "";
        }
        CategoryDTO category = categoryOptional.get();

        Optional<StoreDTO> storeOptional = daoContainer.stockDAO.findStoreWithMostProductsByCategory(category.getId());
        if (storeOptional.isEmpty()) {
            log.error("No store found with products of type {}.", productType);
            return "";
        }
        StoreDTO storeWithMostProducts = storeOptional.get();
        return storeWithMostProducts.toString();
    }

    private static class DAOContainer {

        final Dao<StoreDTO> storeDAO;
        final Dao<CategoryDTO> categoryDAO;
        final Dao<ProductDTO> productDAO;
        final StockDAO stockDAO;

        DAOContainer(Dao<StoreDTO> storeDAO, Dao<CategoryDTO> categoryDAO,
                     Dao<ProductDTO> productDAO, StockDAO stockDAO) {
            this.storeDAO = storeDAO;
            this.categoryDAO = categoryDAO;
            this.productDAO = productDAO;
            this.stockDAO = stockDAO;
        }

    }

    private static DAOContainer initializeDAOs(Connection connection) throws SQLException {
        return new DAOContainer(
                new StoreDAO(connection),
                new CategoryDAO(connection),
                new ProductDAO(connection),
                new StockDAO(connection)
        );
    }
}
