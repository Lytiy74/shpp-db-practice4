package shpp.azaika;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.CategoryDAO;
import shpp.azaika.dao.ProductDAO;
import shpp.azaika.dao.StockDAO;
import shpp.azaika.dao.StoreDAO;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.BatchExecutor;
import shpp.azaika.util.DTOGenerator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) throws IOException, SQLException {
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
             ExecutorService generatorExecutorService = Executors.newFixedThreadPool(2);
             ExecutorService daoExecutorService = Executors.newFixedThreadPool(2)){
            log.info("Generating {} stores, {} categories, {} products and {} stocks...", storesQuantity, categoriesQuantity, productsQuantity, stocksQuantity);

            DTOGenerator generator = new DTOGenerator();

            BlockingQueue<CategoryDTO> categoriesGeneratedQueue = new LinkedBlockingQueue<>();
            BlockingQueue<StoreDTO> storesGeneratedQueue = new LinkedBlockingQueue<>();
            BlockingQueue<ProductDTO> productsGeneratedQueue = new LinkedBlockingQueue<>();
            BlockingQueue<StockDTO> stocksGeneratedQueue = new LinkedBlockingQueue<>();
            BlockingQueue<Long> storesIds  = new LinkedBlockingQueue<>();
            BlockingQueue<Long> categoriesIds  = new LinkedBlockingQueue<>();
            BlockingQueue<Long> productsIds  = new LinkedBlockingQueue<>();
            BlockingQueue<Long> stocksIds  = new LinkedBlockingQueue<>();

            CategoryDAO categoryDAO = new CategoryDAO(connection);
            StoreDAO storeDAO = new StoreDAO(connection);
            ProductDAO productDAO = new ProductDAO(connection);
            StockDAO stockDAO = new StockDAO(connection);

            generatorExecutorService.submit(() -> {
                generator.generateCategoriesToQueue(categoriesQuantity, categoriesGeneratedQueue);
            });

            generatorExecutorService.submit(() -> {
                generator.generateStoresToQueue(storesQuantity, storesGeneratedQueue);
            });

            BatchExecutor<CategoryDTO> categoryDTOBatchExecutor = new BatchExecutor<>(categoriesGeneratedQueue, categoriesIds, categoryDAO, 1000);
            daoExecutorService.submit(categoryDTOBatchExecutor);

            BatchExecutor<StoreDTO> storeDTOBatchExecutor = new BatchExecutor<>(storesGeneratedQueue, storesIds, storeDAO, 1000);
            daoExecutorService.submit(storeDTOBatchExecutor);

            //Генерувати продукти і стоки потрібно лише після пушу магазинів та категорії на бд, щоб отримати індекси



        } catch (SQLException e) {
            log.error("Database error occurred", e);
        }
    }

    private static String getStoreWithMostProductsOfType(CategoryDAO categoryDAO, StockDAO stockDAO, String productType) throws SQLException {

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

}
