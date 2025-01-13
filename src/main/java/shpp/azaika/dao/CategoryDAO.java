package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CategoryDAO {
    private static final Logger log = LoggerFactory.getLogger(CategoryDAO.class);

    private final Connection connection;

    public CategoryDAO(Connection connection) {
        this.connection = connection;
    }

    public List<Short> insertBatch(List<CategoryDTO> dtos) {
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

    public List<Short> insertInChunks(List<CategoryDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Short> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<CategoryDTO> chunk = dtos.subList(i, end);
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

    public Optional<CategoryDTO> findByName(String name) throws SQLException {
        log.info("Find category by name '{}'", name);
        String sql = "SELECT id, name FROM categories WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    log.info("Category '{}' found", name);
                    return Optional.of(new CategoryDTO(resultSet.getShort("id"), resultSet.getString("name")));
                }
            }
        }
        log.warn("Category '{}' not found", name);
        return Optional.empty();
    }

}
