package shpp.azaika.util;


import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class DTOGenerator {

    private static final Logger log = LoggerFactory.getLogger(DTOGenerator.class);

    private final DTOFaker faker = new DTOFaker();
    private final Random random = new Random();

    public Set<StoreDTO> generateStores(int storesQty) {
        Set<StoreDTO> stores = new HashSet<>();
        for (int i = 0; i < storesQty; i++) {
            StoreDTO store = faker.generateStoreDTO();
            stores.add(store);
        }
        log.info("Generated {} stores", storesQty);
        return stores;
    }

    public Set<CategoryDTO> generateCategories(int categoryQty) {
        Set<CategoryDTO> categories = new HashSet<>();
        for (int i = 0; i < categoryQty; i++) {
            CategoryDTO category = faker.generateCategoryDTO();
            categories.add(category);
        }
        log.info("Generated {} categories", categoryQty);
        return categories;
    }

    public Set<ProductDTO> generateProducts(int productsQty, List<Long> categoriesIds) {
        Set<ProductDTO> products = new HashSet<>();
        if (categoriesIds.isEmpty()) {
            log.warn("No categories id`s found. Generate categories first.");
            throw new IllegalStateException("No categories id`s found");
        }

        for (int i = 0; i < productsQty; i++) {
            long randomCategoryId = categoriesIds.get(random.nextInt(categoriesIds.size()));
            ProductDTO product = faker.generateProductDTO(randomCategoryId);
            products.add(product);
        }
        log.info("Generated {} products", productsQty);

        return products;
    }

    public Set<StockDTO> generateStocks(int stockQty, List<Long> storesIds, List<Long> productsIds) {
        Set<StockDTO> stocks = new HashSet<>();
        Set<Pair<Long,Long>> stockKeys = new HashSet<>();
        if (storesIds.isEmpty()|| productsIds.isEmpty()) {
            log.warn("No stores or products found. Generate them first.");
            throw new IllegalStateException("No stores or products found");
        }

        for (int i = 0; i < stockQty; i++) {
            long randomStoreId = storesIds.get(random.nextInt(storesIds.size()));
            long randomProductId = productsIds.get(random.nextInt(productsIds.size()));
            Pair<Long, Long> stockKey = Pair.of(randomStoreId, randomProductId);
            if (!stockKeys.contains(stockKey)) {
                StockDTO stock = faker.generateStockDTO(randomStoreId, randomProductId);
                stocks.add(stock);
                stockKeys.add(stockKey);
            } else {
                i--;
            }
        }
        return stocks;
    }

}
