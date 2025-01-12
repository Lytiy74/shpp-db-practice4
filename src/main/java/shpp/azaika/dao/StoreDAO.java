package shpp.azaika.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.StoreDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class StoreDAO {
    private static final Logger log = LoggerFactory.getLogger(StoreDAO.class);

    private final Connection connection;

    public StoreDAO(Connection connection) {
        this.connection = connection;
    }


    public List<Short> insertBatch(List<StoreDTO> dtos) {
        String sql = "INSERT INTO stores (address) VALUES (?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
            for (StoreDTO dto : dtos) {
                stmt.setString(1, dto.getAddress());
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
            return retrieveIds(stmt.getGeneratedKeys());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Short> insertInChunks(List<StoreDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Short> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<StoreDTO> chunk = dtos.subList(i, end);
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
