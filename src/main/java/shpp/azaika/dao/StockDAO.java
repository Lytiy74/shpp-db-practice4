package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StockDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class StockDAO {
    private static final Logger log = LoggerFactory.getLogger(StockDAO.class);

    private final Connection connection;

    public StockDAO(Connection connection) {
        this.connection = connection;
    }

    public List<Long> insertBatch(List<StockDTO> dtos) {
        String sql = "INSERT INTO stock (shop_id, product_id, quantity) VALUES (?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            for (StockDTO dto : dtos) {
                stmt.setLong(1, dto.getShopId());
                stmt.setLong(2, dto.getProductId());
                stmt.setLong(3, dto.getQuantity());
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
            return retrieveIds(stmt.getGeneratedKeys());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Long> insertInChunks(List<StockDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<StockDTO> chunk = dtos.subList(i, end);
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private List<Long> retrieveIds(ResultSet generatedKeys) throws SQLException {
        List<Long> ids = new ArrayList<>();
        while (generatedKeys.next()){
            ids.add(generatedKeys.getLong(1));
        }
        return ids;
    }
}
