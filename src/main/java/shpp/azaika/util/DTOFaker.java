package shpp.azaika.util;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;
import shpp.azaika.dto.StockDTO;

import java.util.*;

public class DTOFaker {
    private static final Logger log = LoggerFactory.getLogger(DTOFaker.class);
    private final Faker faker;
    private final Set<String> categories;

    public DTOFaker() {
        this.faker = new Faker(Locale.of("uk_UA"));
        this.categories = new HashSet<>();
    }

    public StoreDTO generateStore() {
        String address = faker.address().fullAddress();
        StoreDTO store = new StoreDTO(address);
        log.info("Generated Store: {}", store);
        return store;
    }

    public CategoryDTO generateCategory() {
        String categoryName;
        do {
            categoryName = faker.commerce().department();

        }while (categories.contains(categoryName));
        CategoryDTO category = new CategoryDTO(categoryName);
        categories.add(categoryName);
        log.info("Generated Category: {}", category);
        return category;
    }

    public ProductDTO generateProduct(long categoryId) {
        String productName = faker.commerce().productName();
        double price = faker.number().randomDouble(2, 1, 100_000);
        ProductDTO product = new ProductDTO(categoryId, productName, price);
        log.info("Generated Product: {}", product);
        return product;
    }

    public StockDTO generateStock(long shopId, long productId) {
        int quantity = faker.number().numberBetween(1, 100_000);
        StockDTO stock = new StockDTO(shopId, productId, quantity);
        log.info("Generated Stock: {}", stock);
        return stock;
    }

}
