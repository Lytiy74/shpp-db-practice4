package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.ProductDTO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ProductDAO implements Dao<ProductDTO> {
    private static final Logger log = LoggerFactory.getLogger(ProductDAO.class);
    private final Connection connection;

    private final List<ProductDTO> batch = new ArrayList<>();

    public ProductDAO(Connection connection) {
        this.connection = connection;
        log.info("ProductDAO initialized");
    }

    @Override
    public Optional<ProductDTO> get(long id) throws SQLException {
        log.info("Get product with id {}", id);
        String sql = "SELECT id, category_id, name, price FROM products WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, id);
            try(ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    log.info("Found product with id {}", id);
                    return Optional.of(new ProductDTO(rs.getLong("id"), rs.getLong("category_id"), rs.getString("name"), rs.getDouble("price")));
                }
            }
        }
        log.info("Product with id {} not found", id);
        return Optional.empty();
    }

    @Override
    public List<ProductDTO> getAll() throws SQLException {
        log.info("Get all products");
        String sql = "SELECT id, category_id, name, price FROM products";
        List<ProductDTO> productDTOS = new ArrayList<>();
        try(PreparedStatement ps = connection.prepareStatement(sql)) {
            try(ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    productDTOS.add(new ProductDTO(rs.getLong("id"), rs.getLong("category_id"), rs.getString("name"), rs.getDouble("price")));
                }
            }
        }
        log.info("Found {} products dtos", productDTOS.size());
        return productDTOS;
    }

    @Override
    public void save(ProductDTO productDTO) throws SQLException {
        log.info("Saving product {}", productDTO);
        String sql = "INSERT INTO products (category_id, name, price) VALUES (?, ?, ?)";
        try(PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, productDTO.getCategoryId());
            ps.setString(2, productDTO.getName());
            ps.setDouble(3, productDTO.getPrice());
            ps.executeUpdate();
        }
    }

    @Override
    public void update(ProductDTO productDTO) throws SQLException {
        log.info("Updating product {}", productDTO);
        String sql = "UPDATE products SET name = ?, price = ? WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, productDTO.getName());
            ps.setDouble(2, productDTO.getPrice());
            ps.setLong(3, productDTO.getId());
            ps.executeUpdate();
        }
    }

    @Override
    public void delete(long id) throws SQLException {
        log.info("Deleting product with id {}", id);
        String sql = "DELETE FROM products WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    @Override
    public void addToBatch(ProductDTO dto){
        batch.add(dto);
    }

    @Override
    public void executeBatch() throws SQLException{
        log.info("Execute batch");
        String sql = "INSERT INTO products (name, category_id, price) VALUES (?, ?, ?)";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            for(ProductDTO dto : batch){
                ps.setString(1, dto.getName());
                ps.setLong(2, dto.getCategoryId());
                ps.setDouble(3, dto.getPrice());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        batch.clear();
    }

    @Override
    public Optional<ProductDTO> findByName(String name) throws SQLException {
        log.info("Find product by name '{}'", name);
        String sql = "SELECT id, name, category_id, price FROM products WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    log.info("Product '{}' found", name);
                    return Optional.of(new ProductDTO(resultSet.getLong("id"), resultSet.getLong("category_id"),resultSet.getString("name"), resultSet.getDouble("price")));
                }
            }
        }
        log.warn("Product '{}' not found", name);
        return Optional.empty();
    }
}
