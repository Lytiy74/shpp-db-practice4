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
}
