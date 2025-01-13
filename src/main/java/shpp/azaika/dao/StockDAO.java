package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.StockDTO;
import shpp.azaika.dto.StoreDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StockDAO {
    private static final Logger log = LoggerFactory.getLogger(StockDAO.class);

    private final Connection connection;

    public StockDAO(Connection connection) {
        this.connection = connection;
    }

    public List<Short> insertBatch(List<StockDTO> dtos) {
        String sql = "INSERT INTO stocks (shop_id, product_id, quantity) VALUES (?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            for (StockDTO dto : dtos) {
                stmt.setShort(1, dto.getShopId());
                stmt.setShort(2, dto.getProductId());
                stmt.setInt(3, dto.getQuantity());
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
            return retrieveIds(stmt.getGeneratedKeys());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Short> insertInChunks(List<StockDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Short> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<StockDTO> chunk = dtos.subList(i, end);
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private List<Short> retrieveIds(ResultSet generatedKeys) throws SQLException {
        List<Short> ids = new ArrayList<>();
        while (generatedKeys.next()){
            ids.add(generatedKeys.getShort(1));
        }
        return ids;
    }

    public Optional<StoreDTO> findStoreWithMostProductsByCategory(long categoryId) throws SQLException {
        log.info("Find store with most products for category ID {}", categoryId);
        String sql = """
        SELECT s.id, s.address, COUNT(*) AS product_count
        FROM stocks st
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
                    return Optional.of(new StoreDTO(resultSet.getShort("id"), resultSet.getString("address")));
                }
            }
        }
        log.warn("No store found for category ID {}", categoryId);
        return Optional.empty();
    }

}
