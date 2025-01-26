package shpp.azaika.dto;

import jakarta.validation.constraints.PositiveOrZero;

import java.util.UUID;

public class ShopByCategoryDTO {
    private final UUID categoryId;
    private final UUID shopId;
    @PositiveOrZero
    private final int quantity;

    public ShopByCategoryDTO(UUID categoryId, UUID shopId, int quantity) {
        this.categoryId = categoryId;
        this.shopId = shopId;
        this.quantity = quantity;
    }

    public UUID getShopId() {
        return shopId;
    }

    public UUID getCategoryId() {
        return categoryId;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "ShopByCategoryDTO{" +
                "categoryId=" + categoryId +
                ", shopId=" + shopId +
                ", quantity=" + quantity +
                '}';
    }
}
