package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;

public interface DAOFactory<T> {
    DAO<T> create(CqlSession session);
}
