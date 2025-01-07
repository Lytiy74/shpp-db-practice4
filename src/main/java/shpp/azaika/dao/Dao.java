package shpp.azaika.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface Dao<T> {
    Optional<T> get(long id) throws SQLException;

    List<T> getAll() throws SQLException;

    void save(T t) throws SQLException;

    void update(T t) throws SQLException;

    void delete(long id) throws SQLException;

    void addToBatch(T t);

    List<Long> executeBatch() throws SQLException;

    Optional<T> findByName(String name) throws SQLException;
}
