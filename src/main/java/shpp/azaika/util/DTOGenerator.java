package shpp.azaika.util;


import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DTOGenerator {

    private static final Logger log = LoggerFactory.getLogger(DTOGenerator.class);

    private final DTOFaker faker = new DTOFaker();
    private final Random random = new Random();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    public List<StoreDTO> generateAndValidateStores(int storesQty) {
        List<StoreDTO> stores = new ArrayList<>();
        for (int i = 0; i < storesQty; i++) {
            StoreDTO store = faker.generateStoreDTO();
            if (!validator.validate(store).isEmpty()){
                i--;
                continue;
            }
            stores.add(store);
        }
        log.info("Generated {} stores", storesQty);
        return stores;
    }

    public List<CategoryDTO> generateAndValidateCategories(int categoryQty) {
        List<CategoryDTO> categories = new ArrayList<>();
        for (int i = 0; i < categoryQty; i++) {
            CategoryDTO category = faker.generateCategoryDTO();
            if (!validator.validate(category).isEmpty()){
                i--;
                continue;
            }
            categories.add(category);
        }
        log.info("Generated {} categories", categoryQty);
        return categories;
    }

    public List<ProductDTO> generateAndValidateProducts(int productsQty, List<Short> categoriesIds) {
        List<ProductDTO> products = new ArrayList<>();
        if (categoriesIds.isEmpty()) {
            log.warn("No categories id`s found. Generate categories first.");
            throw new IllegalStateException("No categories id`s found");
        }

        for (int i = 0; i < productsQty; i++) {
            short randomCategoryId = categoriesIds.get(random.nextInt(categoriesIds.size()));
            ProductDTO product = faker.generateProductDTO(randomCategoryId);
            if (!validator.validate(product).isEmpty()){
                i--;
                continue;
            }
            products.add(product);
        }
        log.info("Generated {} products", productsQty);

        return products;
    }


}
