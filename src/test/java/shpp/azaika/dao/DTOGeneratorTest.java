package shpp.azaika.util;

import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class DTOGeneratorTest {

    @Mock
    private DTOFaker faker;

    @Mock
    private Validator validator;

    private DTOGenerator generator;

    private final Random random = new Random();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        generator = new DTOGenerator(faker, random, validator);
    }

    @Test
    void generateAndValidateStores_success() {
        StoreDTO storeMock = mock(StoreDTO.class);
        when(faker.generateStoreDTO()).thenReturn(storeMock);
        when(validator.validate(storeMock)).thenReturn(Collections.emptySet());

        List<StoreDTO> result = generator.generateAndValidateStores(3);

        assertEquals(3, result.size());
        verify(faker, times(3)).generateStoreDTO();
        verify(validator, times(3)).validate(storeMock);
    }

    @Test
    void generateAndValidateCategories_success() {
        CategoryDTO categoryMock = mock(CategoryDTO.class);
        when(faker.generateCategoryDTO()).thenReturn(categoryMock);
        when(validator.validate(categoryMock)).thenReturn(Collections.emptySet());

        List<CategoryDTO> result = generator.generateAndValidateCategories(2);

        assertEquals(2, result.size());
        verify(faker, times(2)).generateCategoryDTO();
        verify(validator, times(2)).validate(categoryMock);
    }

    @Test
    void generateAndValidateProducts_success() {
        ProductDTO productMock = mock(ProductDTO.class);
        when(faker.generateProductDTO(anyShort())).thenReturn(productMock);
        when(validator.validate(productMock)).thenReturn(Collections.emptySet());

        List<Short> categoryIds = List.of((short) 1, (short) 2);

        List<ProductDTO> result = generator.generateAndValidateProducts(5, categoryIds);

        assertEquals(5, result.size());
        verify(faker, times(5)).generateProductDTO(anyShort());
        verify(validator, times(5)).validate(productMock);
    }

    @Test
    void generateAndValidateProducts_emptyCategoryIds() {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> generator.generateAndValidateProducts(3, new ArrayList<>()));

        assertEquals("No categories id`s found", exception.getMessage());
    }


}
