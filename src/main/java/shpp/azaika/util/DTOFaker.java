package shpp.azaika.util;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.ShopByCategoryDTO;
import shpp.azaika.dto.StoreDTO;

import java.math.BigDecimal;
import java.util.*;

public class DTOFaker {
    private static final Logger log = LoggerFactory.getLogger(DTOFaker.class);

    private static final int MAX_PRICE = 100_000;
    private static final int MIN_PRICE = 1;
    private static final int MAX_STOCK_QUANTITY = 500;

    private final ThreadLocal<Faker> threadLocalFaker = ThreadLocal.withInitial(() -> new Faker(Locale.of("uk_UA")));
    private final Set<String> usedCategories = Collections.synchronizedSet(new HashSet<>());

    public StoreDTO generateStoreDTO() {
        String address = threadLocalFaker.get().address().fullAddress();
        StoreDTO store = new StoreDTO(UUID.randomUUID(), address);
        log.debug("Generated Store: {}", store);
        return store;
    }

    public CategoryDTO generateCategoryDTO() {
        String categoryName;
        do {
            categoryName = threadLocalFaker.get().commerce().department();
        } while (!usedCategories.add(categoryName));
        CategoryDTO category = new CategoryDTO(UUID.randomUUID(), categoryName);
        log.debug("Generated Category: {}", category);
        return category;
    }

    public ProductDTO generateProductDTO(UUID categoryId) {
        String productName = threadLocalFaker.get().commerce().productName();
        BigDecimal price = BigDecimal.valueOf(threadLocalFaker.get().number().numberBetween(MIN_PRICE, MAX_PRICE));
        ProductDTO product = new ProductDTO(UUID.randomUUID(), categoryId, productName, price);
        log.debug("Generated Product: {}", product);
        return product;
    }

    public ShopByCategoryDTO generateShopByCategoryDTO(UUID categoryId, UUID shopId) {
        short quantity = (short) threadLocalFaker.get().number().numberBetween(1, MAX_STOCK_QUANTITY);
        ShopByCategoryDTO stock = new ShopByCategoryDTO(categoryId, shopId, quantity);
        log.debug("Generated Stock: {}", stock);
        return stock;
    }

}




