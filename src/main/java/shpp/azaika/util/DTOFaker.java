package shpp.azaika.util;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.Locale;

public class DTOFaker {
    private static final Logger log = LoggerFactory.getLogger(DTOFaker.class);
    private final Faker faker;

    public DTOFaker() {
        this.faker = new Faker(Locale.of("uk_UA"));
    }

    public StoreDTO generateStore() {
        return new StoreDTO(faker.address().fullAddress());
    }

    public CategoryDTO generateCategory() {
        return new CategoryDTO(faker.commerce().department());
    }
}
