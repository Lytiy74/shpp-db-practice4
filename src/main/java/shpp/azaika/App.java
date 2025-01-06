package shpp.azaika;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dao.*;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.util.DTOFaker;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static int storesQuantity;
    private static int categoriesQuantity;
    private static int productsQuantity;
    private static int stocksQuantity;

    public static void main(String[] args) throws IOException {
        String productType = args.length > 0 ? args[0] : "default";
        Properties generationProperties = new Properties();
        generationProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("generation.properties"));
        storesQuantity = Integer.parseInt(generationProperties.getProperty("stores.quantity"));
        categoriesQuantity = Integer.parseInt(generationProperties.getProperty("categories.quantity"));
        productsQuantity = Integer.parseInt(generationProperties.getProperty("products.quantity"));
        stocksQuantity = Integer.parseInt(generationProperties.getProperty("stock.quantity"));

        Properties dbProperties = new Properties();
        dbProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties"));

        try (Connection connection = DriverManager.getConnection(
                dbProperties.getProperty("db.url"), dbProperties.getProperty("db.user"), dbProperties.getProperty("db.password"));
             ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {

            log.info("Starting application...");

            DAOContainer daoContainer = initializeDAOs(connection);
            DTOFaker faker = new DTOFaker();
            Validator validator = factory.getValidator();

            generateStores(faker, daoContainer.storeDAO, validator);
            generateCategories(faker, daoContainer.categoryDAO, validator);
            generateProducts(faker, daoContainer.categoryDAO, daoContainer.productDAO, validator);
            generateStocks(faker, daoContainer.productDAO, daoContainer.storeDAO, daoContainer.stockDAO);

            log.info("Querying store with most products of type: {}", productType);
            String storeAddress = getStoreWithMostProductsOfType(daoContainer, productType);
            log.info("Store with most products of type {}: {}", productType, storeAddress);

        } catch (SQLException e) {
            log.error("Database error occurred", e);
        }
    }


    private static void generateStores(DTOFaker faker, Dao<StoreDTO> storeDAO, Validator validator) throws SQLException {
        log.info("Generating and saving stores...");
        for (int i = 0; i < storesQuantity; i++) {
            StoreDTO storeDTO = faker.generateStore();
            validateAndSave(storeDTO, storeDAO, validator);
        }
    }

    private static void generateCategories(DTOFaker faker, Dao<CategoryDTO> categoryDAO, Validator validator) throws SQLException {
        log.info("Generating and saving categories...");
        for (int i = 0; i < categoriesQuantity; i++) {
            CategoryDTO categoryDTO = faker.generateCategory();
            validateAndSave(categoryDTO, categoryDAO, validator);
        }
    }

    private static void generateProducts(DTOFaker faker, Dao<CategoryDTO> categoryDAO, Dao<ProductDTO> productDAO, Validator validator) throws SQLException {
        log.info("Generating and saving products...");
        List<CategoryDTO> categories = categoryDAO.getAll();
        for (int i = 0; i < productsQuantity; i++) {
            long randomCategoryId = ThreadLocalRandom.current().nextLong(1, categories.size());
            ProductDTO productDTO = faker.generateProduct(randomCategoryId);
            validateAndSave(productDTO, productDAO, validator);
        }
    }

    private static void generateStocks(DTOFaker faker, Dao<ProductDTO> productDAO, Dao<StoreDTO> storeDAO, MultipleIdDao<StockDTO> stockDAO) throws SQLException {
        log.info("Generating and saving stocks...");
        List<ProductDTO> products = productDAO.getAll();
        List<StoreDTO> stores = storeDAO.getAll();
        for (int i = 0; i < stocksQuantity; i++) {
            long randomProductId = products.get(i % products.size()).getId();
            long randomStoreId = stores.get(i % stores.size()).getId();
            StockDTO stockDTO = faker.generateStock(randomStoreId, randomProductId);
            stockDAO.save(stockDTO);
        }
    }

    private static <T> void validateAndSave(T dto, Dao<T> dao, Validator validator) throws SQLException {
        var violations = validator.validate(dto);
        if (violations.isEmpty()) {
            dao.save(dto);
        } else {
            violations.forEach(violation -> log.error("Validation failed: {}", violation.getMessage()));
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
