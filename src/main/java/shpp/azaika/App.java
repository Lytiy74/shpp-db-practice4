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
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.DTOGenerator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final int chunkSize = 3000;

    public static void main(String[] args) throws IOException, SQLException, ExecutionException, InterruptedException {
        log.info("Starting application...");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String productType = args.length > 0 ? args[0] : "Взуття";

        Properties generationProperties = new Properties();
        generationProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));

        int storesQuantity = Integer.parseInt(generationProperties.getProperty("stores.quantity"));
        int categoriesQuantity = Integer.parseInt(generationProperties.getProperty("categories.quantity"));
        int productsQuantity = Integer.parseInt(generationProperties.getProperty("products.quantity"));
        int stocksQuantity = Integer.parseInt(generationProperties.getProperty("stock.quantity"));

        DataSource dataSource = DataSource.getInstance();
        DTOGenerator dtoGenerator = new DTOGenerator();

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        stopWatch.reset();
        stopWatch.start();
        List<StoreDTO> storeDTOS = dtoGenerator.generateStores(storesQuantity);
        stopWatch.stop();
        log.info("Generated stores in {} ms", stopWatch.getDuration().toMillis());

        StoreDAO storeDAO = new StoreDAO(dataSource.getConnection());
        stopWatch.reset();
        stopWatch.start();
        Future<List<Long>> storeInsertTask = executorService.submit(() -> storeDAO.insertInChunks(storeDTOS, chunkSize));
        List<Long> storeIds = storeInsertTask.get();
        stopWatch.stop();
        log.info("Inserted stores in DB in {} ms", stopWatch.getDuration().toMillis());

        stopWatch.reset();
        stopWatch.start();
        List<CategoryDTO> categoryDTOS = dtoGenerator.generateCategories(categoriesQuantity);
        stopWatch.stop();
        log.info("Generated categories in {} ms", stopWatch.getDuration().toMillis());

        CategoryDAO categoryDAO = new CategoryDAO(dataSource.getConnection());
        stopWatch.reset();
        stopWatch.start();
        Future<List<Long>> categoryInsertTask = executorService.submit(() -> categoryDAO.insertInChunks(categoryDTOS, chunkSize));
        List<Long> categoryIds = categoryInsertTask.get();
        stopWatch.stop();
        log.info("Inserted categories in DB in {} ms", stopWatch.getDuration().toMillis());

        stopWatch.reset();
        stopWatch.start();
        List<ProductDTO> productDTOS = dtoGenerator.generateProducts(productsQuantity, categoryIds);
        stopWatch.stop();
        log.info("Generated products in {} ms", stopWatch.getDuration().toMillis());

        ProductDAO productDAO = new ProductDAO(dataSource.getConnection());
        stopWatch.reset();
        stopWatch.start();
        Future<List<Long>> productInsertTask = executorService.submit(() -> productDAO.insertInChunks(productDTOS, chunkSize));
        List<Long> productIds = productInsertTask.get();
        stopWatch.stop();
        log.info("Inserted products in DB in {} ms", stopWatch.getDuration().toMillis());

        stopWatch.reset();
        stopWatch.start();
        List<StockDTO> stockDTOS = dtoGenerator.generateStocks(stocksQuantity, storeIds, productIds);
        stopWatch.stop();
        log.info("Generated stocks in {} ms", stopWatch.getDuration().toMillis());

        StockDAO stockDAO = new StockDAO(dataSource.getConnection());
        stopWatch.reset();
        stopWatch.start();
        for (int i = 0; i < stockDTOS.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, stockDTOS.size());
            List<StockDTO> chunk = stockDTOS.subList(i, end);
            stockDAO.insertInChunks(chunk, chunkSize);
            log.info("Inserted {} stocks",i);
        }
        stopWatch.stop();
        log.info("Inserted stocks in DB in {} ms", stopWatch.getDuration().toMillis());

        executorService.shutdown();
        log.info("Application finished in {} ms", stopWatch.getDuration().toMillis());
    }
}
