package shpp.azaika.util;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class DTOFaker {
    private static final Logger log = LoggerFactory.getLogger(DTOFaker.class);

    private static final int MAX_PRICE = 100_000;
    private static final int MIN_PRICE = 1;
    private static final int MAX_STOCK_QUANTITY = 500;

    private final ThreadLocal<Faker> threadLocalFaker = ThreadLocal.withInitial(() -> new Faker(Locale.of("uk_UA")));
    private final Set<String> usedCategories = Collections.synchronizedSet(new HashSet<>());

    public StoreDTO generateStoreDTO() {
        String address = threadLocalFaker.get().address().fullAddress();
        StoreDTO store = new StoreDTO(address);
        log.info("Generated Store: {}", store);
        return store;
    }

    public CategoryDTO generateCategoryDTO() {
        String categoryName;
        do {
            categoryName = threadLocalFaker.get().commerce().department();
        } while (!usedCategories.add(categoryName));
        CategoryDTO category = new CategoryDTO(categoryName);
        log.info("Generated Category: {}", category);
        return category;
    }

    public ProductDTO generateProductDTO(long categoryId) {
        String productName = threadLocalFaker.get().commerce().productName();
        long price = threadLocalFaker.get().number().numberBetween(MIN_PRICE, MAX_PRICE);
        ProductDTO product = new ProductDTO(categoryId, productName, price);
        log.info("Generated Product: {}", product);
        return product;
    }

    public StockDTO generateStockDTO(long storeId, long productId) {
        long quantity = threadLocalFaker.get().number().numberBetween(1, MAX_STOCK_QUANTITY);
        StockDTO stock = new StockDTO(storeId, productId, quantity);
        log.info("Generated Stock: {}", stock);
        return stock;
    }

}




