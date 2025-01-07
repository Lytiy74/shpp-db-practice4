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
import shpp.azaika.util.DTOGenerator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

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
             ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            Validator validator = factory.getValidator();


            DAOContainer daoContainer = initializeDAOs(connection);
            log.info("Generating {} stores, {} categories, {} products and {} stocks...", storesQuantity, categoriesQuantity, productsQuantity, stocksQuantity);
            DTOGenerator generator = new DTOGenerator();

            Set<CategoryDTO> categoryDTOS = generator.generateCategories(categoriesQuantity);
            for (CategoryDTO categoryDTO : categoryDTOS) {
                daoContainer.categoryDAO.addToBatch(categoryDTO);
            }
            List<Long> categoriesIds = daoContainer.categoryDAO.executeBatch();

            Set<StoreDTO> storeDTOS = generator.generateStores(storesQuantity);
            for (StoreDTO storeDTO : storeDTOS) {
                daoContainer.storeDAO.addToBatch(storeDTO);
            }
            List<Long> storesIds = daoContainer.storeDAO.executeBatch();

            Set<ProductDTO> productDTOS = generator.generateProducts(productsQuantity,categoriesIds);
            for (ProductDTO productDTO : productDTOS) {
                daoContainer.productDAO.addToBatch(productDTO);
            }
            List<Long> productsIds = daoContainer.productDAO.executeBatch();

            Set<StockDTO> stockDTOS = generator.generateStocks(stocksQuantity,storesIds,productsIds);
            for (StockDTO stockDTO : stockDTOS) {
                daoContainer.stockDAO.addToBatch(stockDTO);
            }
            List<Long> stocksIds = daoContainer.stockDAO.executeBatch();


            log.info("Querying store with most products of type: {}", productType);
            String storeAddress = getStoreWithMostProductsOfType(daoContainer, productType);
            log.info("Store with most products of type {}: {}", productType, storeAddress);

        } catch (SQLException e) {
            log.error("Database error occurred", e);
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

    private static <T> void validateAndAddToBatch(T dto, Dao<T> dao, Validator validator) throws SQLException {
        var violations = validator.validate(dto);
        if (violations.isEmpty()) {
            dao.addToBatch(dto);
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
