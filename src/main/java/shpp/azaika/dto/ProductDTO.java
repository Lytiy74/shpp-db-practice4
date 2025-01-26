package shpp.azaika.dto;

import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.Length;

import java.math.BigDecimal;
import java.util.UUID;

public class ProductDTO {
    private UUID id;
    private final UUID categoryId;
    @Length(min = 5, max = 255)
    private final String name;
    @Positive
    private final BigDecimal price;

    public ProductDTO(UUID id, UUID categoryId, String name, BigDecimal price) {
        this.id = id;
        this.categoryId = categoryId;
        this.name = name;
        this.price = price;
    }

    public ProductDTO(UUID categoryId, String name, BigDecimal price) {
        this.categoryId = categoryId;
        this.name = name;
        this.price = price;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public UUID getCategoryId() {
        return categoryId;
    }

    @Override
    public String toString() {
        return "ProductDTO{" +
                "id=" + id +
                ", categoryId=" + categoryId +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }
}
