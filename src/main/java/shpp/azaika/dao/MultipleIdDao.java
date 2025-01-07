package shpp.azaika.dao;

import shpp.azaika.dto.StockDTO;

import java.sql.SQLException;
import java.util.Optional;

public interface MultipleIdDao <T> extends Dao<T>{
    Optional<StockDTO> get(long shopId, long productId) throws SQLException;

    void delete(long shopId, long productId) throws SQLException;
}
