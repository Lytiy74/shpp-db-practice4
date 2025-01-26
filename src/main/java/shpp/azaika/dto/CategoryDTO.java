package shpp.azaika.dto;

import org.hibernate.validator.constraints.Length;

import java.util.UUID;

public class CategoryDTO {
    private UUID id;
    @Length(min = 5, max = 255)
    private final String categoryName;

    public CategoryDTO(UUID id, String categoryName) {
        this.id = id;
        this.categoryName = categoryName;
    }

    public CategoryDTO(String categoryName) {
        this.categoryName = categoryName;
    }

    public UUID getId() {
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
