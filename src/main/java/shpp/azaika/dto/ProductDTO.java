package shpp.azaika.dto;

import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.Length;

public class ProductDTO {
    private short id;
    private final short categoryId;
    @Length(min = 5, max = 255)
    private final String name;
    @Positive
    private final double price;

    public ProductDTO(short id, short categoryId, String name, double price) {
        this.id = id;
        this.categoryId = categoryId;
        this.name = name;
        this.price = price;
    }

    public ProductDTO(short categoryId, String name, double price) {
        this.categoryId = categoryId;
        this.name = name;
        this.price = price;
    }

    public short getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }

    public short getCategoryId() {
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
