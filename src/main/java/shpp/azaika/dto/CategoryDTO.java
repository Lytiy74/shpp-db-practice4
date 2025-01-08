package shpp.azaika.dto;

import org.hibernate.validator.constraints.Length;

public class CategoryDTO {
    private long id;
    @Length(min = 5, max = 255)
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
