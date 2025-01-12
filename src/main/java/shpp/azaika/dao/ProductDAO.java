package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.ProductDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ProductDAO {
    private static final Logger log = LoggerFactory.getLogger(ProductDAO.class);

    private final Connection connection;

    public ProductDAO(Connection connection) {
        this.connection = connection;
    }

    public List<Short> insertBatch(List<ProductDTO> dtos) {
        String sql = "INSERT INTO products (name, category_id, price) VALUES (?,?,?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            for (ProductDTO dto : dtos) {
                stmt.setString(1, dto.getName());
                stmt.setShort(2, dto.getId());
                stmt.setDouble(3, dto.getPrice());
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
            return retrieveIds(stmt.getGeneratedKeys());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Short> insertInChunks(List<ProductDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Short> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<ProductDTO> chunk = dtos.subList(i, end);
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
