package shpp.azaika.dao;

import shpp.azaika.dto.StoreDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StoreDAO implements Dao<StoreDTO> {
    private final Connection connection;

    public StoreDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Optional<StoreDTO> get(long id) throws SQLException {
        String sql = "SELECT id, address FROM store WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setLong(1, id);
            try(ResultSet rs = ps.executeQuery()){
                if(rs.next()){
                    return Optional.of(new StoreDTO(rs.getLong("id"), rs.getString("address")));
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public List<StoreDTO> getAll() throws SQLException {
        String sql = "SELECT id, address FROM store";
        List<StoreDTO> storeDTOS = new ArrayList<>();
        try(Statement statement = connection.createStatement()){
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()){
                storeDTOS.add(new StoreDTO(resultSet.getLong("id"), resultSet.getString("address")));
            }
        }
        return storeDTOS;
    }

    @Override
    public void save(StoreDTO storeDTO) throws SQLException {
        String sql = "INSERT INTO stores (address) VALUES (?)";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setString(1, storeDTO.getAddress());
            ps.executeUpdate();
        }
    }

    @Override
    public void update(StoreDTO storeDTO) throws SQLException {
        String sql = "UPDATE stores SET address = ? WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setString(1, storeDTO.getAddress());
            ps.setLong(2, storeDTO.getId());
            ps.executeUpdate();
        }
    }

    @Override
    public void delete(long id) throws SQLException {
        String sql = "DELETE FROM stores WHERE id = ?";
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }
}
