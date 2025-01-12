package shpp.azaika.dto;

import jakarta.validation.constraints.PositiveOrZero;

public class StockDTO {
    private final short shopId;
    private final short productId;
    @PositiveOrZero
    private final int quantity;

    public StockDTO(short shopId, short productId, int quantity) {
        this.shopId = shopId;
        this.productId = productId;
        this.quantity = quantity;
    }

    public short getShopId() {
        return shopId;
    }

    public short getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "StockDTO{" +
                "shopId=" + shopId +
                ", productId=" + productId +
                ", quantity=" + quantity +
                '}';
    }
}
