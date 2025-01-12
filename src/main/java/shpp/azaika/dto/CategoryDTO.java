package shpp.azaika.dto;

import org.hibernate.validator.constraints.Length;

public class CategoryDTO {
    private short id;
    @Length(min = 5, max = 255)
    private final String categoryName;

    public CategoryDTO(short id, String categoryName) {
        this.id = id;
        this.categoryName = categoryName;
    }

    public CategoryDTO(String categoryName) {
        this.categoryName = categoryName;
    }

    public short getId() {
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
