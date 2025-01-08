package shpp.azaika.dto;

import jakarta.validation.constraints.PositiveOrZero;

public class StockDTO {
    private final long shopId;
    private final long productId;
    @PositiveOrZero
    private final long quantity;

    public StockDTO(long shopId, long productId, long quantity) {
        this.shopId = shopId;
        this.productId = productId;
        this.quantity = quantity;
    }

    public long getShopId() {
        return shopId;
    }

    public long getProductId() {
        return productId;
    }

    public long getQuantity() {
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
