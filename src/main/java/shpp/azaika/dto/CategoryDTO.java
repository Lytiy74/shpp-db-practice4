package shpp.azaika.dto;

public class CategoryDTO {
    private long id;
    private final String categoryName;

    public CategoryDTO(long id, String categoryName) {
        this.id = id;
        this.categoryName = categoryName;
    }

    public CategoryDTO(String categoryName) {
        this.categoryName = categoryName;
    }

    public long getId() {
        return id;
    }

    public String getCategoryName() {
        return categoryName;
    }

    @Override
    public String toString() {
        return "CategoryDTO{" + "id=" + id +
                ", categoryName='" + categoryName + '\'' +
                '}';
    }
}
