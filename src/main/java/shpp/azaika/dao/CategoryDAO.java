package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CategoryDAO implements Dao<CategoryDTO>{
    private static final Logger log = LoggerFactory.getLogger(CategoryDAO.class);
    private final Connection connection;
    private final List<CategoryDTO> batch = new ArrayList<>();

    public CategoryDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Optional<CategoryDTO> get(long id) throws SQLException {
        log.info("Get category with id {}", id);
        String sql = "SELECT id, name FROM categories WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    log.info("Found category with id {}", id);
                    return Optional.of(new CategoryDTO(resultSet.getLong("id"), resultSet.getString("name")));
                }
            }
        }
        log.warn("Category with id {}, not found", id);
        return Optional.empty();
    }

    @Override
    public List<CategoryDTO> getAll() throws SQLException {
        log.info("Get all categories dtos");
        String sql = "SELECT id, name FROM categories";
        List<CategoryDTO> categoryDTOS = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    categoryDTOS.add(new CategoryDTO(resultSet.getLong("id"), resultSet.getString("name")));
                }
            }
        }
        log.info("Found {} categories dtos", categoryDTOS.size());
        return categoryDTOS;
    }

    @Override
    public void save(CategoryDTO categoryDTO) throws SQLException {
        log.info("Save category {}", categoryDTO);
        String sql = "INSERT INTO categories (name) VALUES ( ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1,categoryDTO.getCategoryName());
            ps.executeUpdate();
        }
    }

    @Override
    public void update(CategoryDTO categoryDTO) throws SQLException {
        log.info("Update category {}", categoryDTO);
        String sql = "UPDATE categories SET name = ? WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1,categoryDTO.getCategoryName());
            ps.setLong(2,categoryDTO.getId());
            ps.executeUpdate();
        }

    }

    @Override
    public void delete(long id) throws SQLException {
        log.info("Delete category with id {}", id);
        String sql = "DELETE FROM categories WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    @Override
    public void addToBatch(CategoryDTO dto){
        batch.add(dto);
    }

    @Override
    public List<Long> executeBatch() throws SQLException{
        log.info("Execute batch");
        String sql = "INSERT INTO categories (name) VALUES (?)";
        List<Long> generatedKeys = new ArrayList<>();
        try(PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            for(CategoryDTO dto : batch){
                ps.setString(1, dto.getCategoryName());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet rs = ps.getGeneratedKeys();
            while(rs.next()){
                generatedKeys.add(rs.getLong(1));
                log.debug("Generated key {}", rs.getLong(1));
            }
        }
        batch.clear();
        return generatedKeys;
    }


    public Optional<CategoryDTO> findByName(String name) throws SQLException {
        log.info("Find category by name '{}'", name);
        String sql = "SELECT id, name FROM categories WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    log.info("Category '{}' found", name);
                    return Optional.of(new CategoryDTO(resultSet.getLong("id"), resultSet.getString("name")));
                }
            }
        }
        log.warn("Category '{}' not found", name);
        return Optional.empty();
    }
}
