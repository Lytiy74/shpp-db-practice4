package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StockDAO implements MultipleIdDao<StockDTO> {
    private static final Logger log = LoggerFactory.getLogger(StockDAO.class);
    private final Connection connection;

    public StockDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Optional<StockDTO> get(long shopId, long productId) throws SQLException {
        log.info("Get stock with shopId {} and productId {}", shopId, productId);
        String sql = "SELECT shop_id, product_id, quantity FROM stock WHERE shop_id = ? AND product_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, shopId);
            ps.setLong(2, productId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    log.info("Found stock with shopId {} and productId {}", shopId, productId);
                    return Optional.of(new StockDTO(rs.getLong("shop_id"), rs.getLong("product_id"), rs.getInt("quantity")));
                }
            }
        }
        log.info("Stock with shopId {} and productId {} not found", shopId, productId);
        return Optional.empty();
    }

    @Override
    public List<StockDTO> getAll() throws SQLException {
        String sql = "SELECT shop_id, product_id, quantity FROM stock";
        List<StockDTO> stockDTOS = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    stockDTOS.add(new StockDTO(rs.getLong("shop_id"), rs.getLong("product_id"), rs.getInt("quantity")));
                }
            }
        }
        log.info("Found {} stocks dtos", stockDTOS.size());
        return stockDTOS;
    }

    @Override
    public void save(StockDTO stockDTO) throws SQLException {
        log.info("Saving stock {}", stockDTO);
        String sql = "INSERT INTO stock (shop_id, product_id, quantity) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, stockDTO.getShopId());
            ps.setLong(2, stockDTO.getProductId());
            ps.setLong(3, stockDTO.getQuantity());
            ps.executeUpdate();
        }
    }

    @Override
    public void update(StockDTO stockDTO) throws SQLException {
        log.info("Updating stock {}", stockDTO);
        String sql = "UPDATE stock SET quantity = ? WHERE shop_id = ? AND product_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, stockDTO.getQuantity());
            ps.setLong(2, stockDTO.getShopId());
            ps.setLong(3, stockDTO.getProductId());
            ps.executeUpdate();
        }
    }

    @Override
    public void delete(long shopId, long productId) throws SQLException {
        String sql = "DELETE FROM stock WHERE shop_id = ? AND product_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, shopId);
            ps.setLong(2, productId);
            ps.executeUpdate();
        }
    }

    @Override
    public Optional<StockDTO> get(long id) throws SQLException {
        throw new UnsupportedOperationException("Single ID fetch not supported for Stock.");
    }

    @Override
    public void delete(long id) throws SQLException {
        throw new UnsupportedOperationException("Single ID delete not supported for Stock.");
    }

    @Override
    public Optional<StockDTO> findByName(String name) throws SQLException {
        throw new UnsupportedOperationException("Find by Name not supported for Stock.");
    }

    public Optional<StoreDTO> findStoreWithMostProductsByCategory(long categoryId) throws SQLException {
        log.info("Find store with most products for category ID {}", categoryId);
        String sql = """
        SELECT s.id, s.address, COUNT(*) AS product_count
        FROM stock st
        JOIN stores s ON st.shop_id = s.id
        JOIN products p ON st.product_id = p.id
        WHERE p.category_id = ?
        GROUP BY s.id, s.address
        ORDER BY product_count DESC
        LIMIT 1
    """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, categoryId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    log.info("Store with most products for category ID {} found", categoryId);
                    return Optional.of(new StoreDTO(resultSet.getLong("id"), resultSet.getString("address")));
                }
            }
        }
        log.warn("No store found for category ID {}", categoryId);
        return Optional.empty();
    }
}
