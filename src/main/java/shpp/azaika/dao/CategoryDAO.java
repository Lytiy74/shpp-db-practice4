package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;
import shpp.azaika.dto.StoreDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CategoryDAO {
    private static final Logger log = LoggerFactory.getLogger(CategoryDAO.class);

    private final Connection connection;

    public CategoryDAO(Connection connection) {
        this.connection = connection;
    }

    public List<Long> insertBatch(List<CategoryDTO> dtos) {
        String sql = "INSERT INTO categories (name) VALUES (?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            for (CategoryDTO dto : dtos) {
                stmt.setString(1, dto.getCategoryName());
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
            return retrieveIds(stmt.getGeneratedKeys());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Long> insertInChunks(List<CategoryDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<CategoryDTO> chunk = dtos.subList(i, end);
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
