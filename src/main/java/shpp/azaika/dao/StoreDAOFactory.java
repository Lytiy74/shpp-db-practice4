package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import shpp.azaika.dto.StoreDTO;

public class StoreDAOFactory implements DAOFactory<StoreDTO> {
    @Override
    public DAO<StoreDTO> create(CqlSession session) {
        return new StoreDAO(session);
    }
}
