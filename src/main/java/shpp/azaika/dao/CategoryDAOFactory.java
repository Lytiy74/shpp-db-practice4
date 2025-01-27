package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import shpp.azaika.dto.CategoryDTO;

public class CategoryDAOFactory implements DAOFactory<CategoryDTO> {
    @Override
    public DAO<CategoryDTO> create(CqlSession session) {
        return new CategoryDAO(session);
    }
}
