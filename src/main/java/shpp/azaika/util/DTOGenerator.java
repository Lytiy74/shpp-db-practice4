package shpp.azaika.util;


import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.*;

public class DTOGenerator {

    private static final Logger log = LoggerFactory.getLogger(DTOGenerator.class);

    private final DTOFaker faker = new DTOFaker();
    private final Random random = new Random();

    private final Map<Long, StoreDTO> stores = new HashMap<>();
    private final Map<Long, CategoryDTO> categories = new HashMap<>();
    private final Map<Long, ProductDTO> products = new HashMap<>();
    private final Map<Pair<Long, Long>, StockDTO> stocks = new HashMap<>();

    public Map<Long, StoreDTO> generateStores(int storesQty) {
        for (int i = 0; i < storesQty; i++) {
            StoreDTO store = faker.generateStoreDTO();
            stores.put(store.getId(), store);
        }
        log.info("Generated {} stores", storesQty);
        return stores;
    }

    public Map<Long, CategoryDTO> generateCategories(int categoryQty) {
        for (int i = 0; i < categoryQty; i++) {
            CategoryDTO category = faker.generateCategoryDTO();
            categories.put(category.getId(), category);
        }
        log.info("Generated {} categories", categoryQty);
        return categories;
    }

    public Map<Long, ProductDTO> generateProducts(int productsQty) {
        if (categories.isEmpty()) {
            log.warn("No categories found. Generate categories first.");
            return new HashMap<>();
        }

        for (int i = 0; i < productsQty; i++) {
            long randomCategoryId = categories.keySet().stream()
                    .skip(random.nextLong(categories.size()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Category is empty"));
            ProductDTO product = faker.generateProductDTO(randomCategoryId);
            products.put(product.getId(), product);
        }
        log.info("Generated {} products", productsQty);
        return products;
    }

    public Map<Pair<Long, Long>, StockDTO> generateStocks(int stockQty) {
        if (stores.isEmpty() || products.isEmpty()) {
            log.warn("No stores or products found. Generate them first.");
            return new HashMap<>();
        }

        List<Long> storeIds = new ArrayList<>(stores.keySet());
        List<Long> productIds = new ArrayList<>(products.keySet());

        for (int i = 0; i < stockQty; i++) {
            long randomStoreId = storeIds.get(random.nextInt(storeIds.size()));
            long randomProductId = productIds.get(random.nextInt(productIds.size()));

            Pair<Long, Long> stockKey = Pair.of(randomStoreId, randomProductId);
            if (!stocks.containsKey(stockKey)) {
                StockDTO stock = faker.generateStockDTO(randomStoreId, randomProductId);
                stocks.put(stockKey, stock);
            } else {
                i--;
            }
        }
        return stocks;
    }

}
