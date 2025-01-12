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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
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

        String productType = args.length > 0 ? args[0] : "Взуття";

        Properties generationProperties = new Properties();
        generationProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));

        int storesQuantity = Integer.parseInt(generationProperties.getProperty("stores.quantity"));
        int categoriesQuantity = Integer.parseInt(generationProperties.getProperty("categories.quantity"));
        int productsQuantity = Integer.parseInt(generationProperties.getProperty("products.quantity"));
        int stocksQuantity = Integer.parseInt(generationProperties.getProperty("stock.quantity"));

        DataSource dataSource = DataSource.getInstance();
        DTOGenerator dtoGenerator = new DTOGenerator();

        dropAndCreateTable(dataSource.getConnection());

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        stopWatch.reset();
        stopWatch.start();
        List<StoreDTO> storeDTOS = dtoGenerator.generateStores(storesQuantity);
        stopWatch.stop();
        log.info("Generated stores in {} ms", stopWatch.getDuration().toMillis());

        StoreDAO storeDAO = new StoreDAO(dataSource.getConnection());
        stopWatch.reset();
        stopWatch.start();
        Future<List<Short>> storeInsertTask = executorService.submit(() -> storeDAO.insertInChunks(storeDTOS, CHUNK_SIZE));
        List<Short> storeIds = storeInsertTask.get();
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
        Future<List<Short>> categoryInsertTask = executorService.submit(() -> categoryDAO.insertInChunks(categoryDTOS, CHUNK_SIZE));
        List<Short> categoryIds = categoryInsertTask.get();
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
        Future<List<Short>> productInsertTask = executorService.submit(() -> productDAO.insertInChunks(productDTOS, CHUNK_SIZE));
        List<Short> productIds = productInsertTask.get();
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
        for (int i = 0; i < stockDTOS.size(); i += CHUNK_SIZE) {
            int end = Math.min(i + CHUNK_SIZE, stockDTOS.size());
            List<StockDTO> chunk = stockDTOS.subList(i, end);
            stockDAO.insertInChunks(chunk, CHUNK_SIZE);
            log.info("Inserted {} stocks",i);
        }
        stopWatch.stop();
        log.info("Inserted stocks in DB in {} ms", stopWatch.getDuration().toMillis());

        executorService.shutdown();
        log.info("Application finished in {} ms", stopWatch.getDuration().toMillis());
    }

    public static void dropAndCreateTable(Connection connection){
        String sql = """
                DROP TABLE IF EXISTS stock,products,categories,stores;
                create table categories
                (
                    id   smallint default nextval('categories_id_seq'::regclass) not null
                        primary key,
                    name varchar(255)                                            not null
                );
                
                alter table categories
                    owner to postgres;
                
                create table products
                (
                    id          smallint default nextval('products_id_seq'::regclass) not null
                        primary key,
                    name        varchar(255)                                          not null,
                    category_id smallint                                              not null
                        constraint products_category_id_foreign
                            references categories,
                    price       numeric                                               not null
                );
                
                alter table products
                    owner to postgres;
                
                create table stores
                (
                    id      smallint default nextval('stores_id_seq'::regclass) not null
                        primary key,
                    address varchar(255)                                        not null
                );
                
                alter table stores
                    owner to postgres;
                
                create table stocks
                (
                    shop_id    smallint not null
                        constraint stock_shop_id_foreign
                            references stores,
                    product_id smallint not null
                        constraint stock_product_id_foreign
                            references products,
                    quantity   integer  not null,
                    constraint stock_pkey
                        primary key (shop_id, product_id)
                );
                
                alter table stocks
                    owner to postgres;
                
                """;
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
