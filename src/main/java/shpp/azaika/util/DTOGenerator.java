package shpp.azaika.util;


import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class DTOGenerator {

    private static final Logger log = LoggerFactory.getLogger(DTOGenerator.class);

    private final DTOFaker faker = new DTOFaker();
    private final Random random = new Random();

    public List<StoreDTO> generateStores(int storesQty) {
        List<StoreDTO> stores = new ArrayList<>();
        for (int i = 0; i < storesQty; i++) {
            StoreDTO store = faker.generateStoreDTO();
            stores.add(store);
        }
        log.info("Generated {} stores", storesQty);
        return stores;
    }

    public List<CategoryDTO> generateCategories(int categoryQty) {
        List<CategoryDTO> categories = new ArrayList<>() {
        };
        for (int i = 0; i < categoryQty; i++) {
            CategoryDTO category = faker.generateCategoryDTO();
            categories.add(category);
        }
        log.info("Generated {} categories", categoryQty);
        return categories;
    }

    public List<ProductDTO> generateProducts(int productsQty, List<Short> categoriesIds) {
        List<ProductDTO> products = new ArrayList<>();
        if (categoriesIds.isEmpty()) {
            log.warn("No categories id`s found. Generate categories first.");
            throw new IllegalStateException("No categories id`s found");
        }

        for (int i = 0; i < productsQty; i++) {
            short randomCategoryId = categoriesIds.get(random.nextInt(categoriesIds.size()));
            ProductDTO product = faker.generateProductDTO(randomCategoryId);
            products.add(product);
        }
        log.info("Generated {} products", productsQty);

        return products;
    }

    public List<StockDTO> generateStocks(int stockQty, List<Short> storesIds, List<Short> productsIds) {
        List<StockDTO> stocks = new ArrayList<>();

        if (storesIds.isEmpty() || productsIds.isEmpty()) {
            log.warn("No stores or products found. Generate them first.");
            throw new IllegalStateException("No stores or products found");
        }

        List<Pair<Short, Short>> allCombinations = new ArrayList<>();
        for (Short storeId : storesIds) {
            for (Short productId : productsIds) {
                allCombinations.add(Pair.of(storeId, productId));
            }
        }

        Collections.shuffle(allCombinations, random);

        int limit = Math.min(stockQty, allCombinations.size());
        for (int i = 0; i < limit; i++) {
            Pair<Short, Short> stockKey = allCombinations.get(i);
            StockDTO stock = faker.generateStockDTO(stockKey.getLeft(), stockKey.getRight());
            stocks.add(stock);
        }

        log.info("Generated {} stocks", stocks.size());
        return stocks;
    }

}
