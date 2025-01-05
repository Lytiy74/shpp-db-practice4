package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.ProductDTO;
import shpp.azaika.dto.StoreDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StoreDAO implements Dao<StoreDTO> {
    private static final Logger log = LoggerFactory.getLogger(StoreDAO.class);
    private final Connection connection;

    public StoreDAO(Connection connection) {
        this.connection = connection;
        log.info("Store DAO initialized");
    }

    @Override
    public Optional<StoreDTO> get(long id) throws SQLException {
        log.info("Get store with id {}", id);
        String sql = "SELECT id, address FROM store WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setLong(1, id);
            try(ResultSet rs = ps.executeQuery()){
                if(rs.next()){
                    log.info("Found store with id {}", id);
                    return Optional.of(new StoreDTO(rs.getLong("id"), rs.getString("address")));
                }
            }
        }
        log.info("Store with id {} not found", id);
        return Optional.empty();
    }

    @Override
    public List<StoreDTO> getAll() throws SQLException {
        log.info("Get all store dtos");
        String sql = "SELECT id, address FROM stores";
        List<StoreDTO> storeDTOS = new ArrayList<>();
        try(Statement statement = connection.createStatement()){
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    storeDTOS.add(new StoreDTO(resultSet.getLong("id"), resultSet.getString("address")));
                }
            }
        }
        log.info("Found {} stores dtos", storeDTOS.size());
        return storeDTOS;
    }

    @Override
    public void save(StoreDTO storeDTO) throws SQLException {
        log.info("Save store {}", storeDTO);
        String sql = "INSERT INTO stores (address) VALUES (?)";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setString(1, storeDTO.getAddress());
            ps.executeUpdate();
        }
    }

    @Override
    public void update(StoreDTO storeDTO) throws SQLException {
        log.info("Update store {}", storeDTO);
        String sql = "UPDATE stores SET address = ? WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setString(1, storeDTO.getAddress());
            ps.setLong(2, storeDTO.getId());
            ps.executeUpdate();
        }
    }

    @Override
    public void delete(long id) throws SQLException {
        log.info("Delete store with id {}", id);
        String sql = "DELETE FROM stores WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    @Override
    public Optional<StoreDTO> findByName(String name) throws SQLException {
        log.info("Find store by name '{}'", name);
        String sql = "SELECT id, name, category_id, price FROM products WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    log.info("Store '{}' found", name);
                    return Optional.of(new StoreDTO(resultSet.getString("address")));
                }
            }
        }
        log.warn("Store '{}' not found", name);
        return Optional.empty();
    }
}
