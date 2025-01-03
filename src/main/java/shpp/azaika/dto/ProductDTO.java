package shpp.azaika.dto;

public class ProductDTO {
    private long id;
    private final long categoryId;
    private final String name;
    private final double price;

    public ProductDTO(long id, long categoryId, String name, double price) {
        this.id = id;
        this.categoryId = categoryId;
        this.name = name;
        this.price = price;
    }

    public ProductDTO(long categoryId, String name, double price) {
        this.categoryId = categoryId;
        this.name = name;
        this.price = price;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }

    public long getCategoryId() {
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
