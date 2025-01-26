package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.CategoryDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class CategoryDAO {
    private static final Logger log = LoggerFactory.getLogger(CategoryDAO.class);

    private final CqlSession connection;

    public CategoryDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<UUID> insertBatch(List<CategoryDTO> dtos) {
        String cqlToCategories = "INSERT INTO \"practical5Keyspace\".categories (category_id, category_name) VALUES (?,?)";
        String cqlToCategoriesIdByName = "INSERT INTO \"practical5Keyspace\".categories_id_by_name (category_name, category_id) VALUES (?,?)";
        PreparedStatement stmt = connection.prepare(cqlToCategories);
        PreparedStatement stmtIdByName = connection.prepare(cqlToCategoriesIdByName);
        for (CategoryDTO dto : dtos) {
            BoundStatement bind = stmt.bind(dto.getId(), dto.getCategoryName());
            BoundStatement bindId = stmtIdByName.bind(dto.getCategoryName(), dto.getId());
            connection.executeAsync(bind);
            connection.executeAsync(bindId);
        }
        return retrieveIds(dtos);
    }

    public List<UUID> insertInChunks(List<CategoryDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<CategoryDTO> chunk = dtos.subList(i, end);
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private List<UUID> retrieveIds(List<CategoryDTO> dtos) {
        List<UUID> ids = new ArrayList<>();
        for (CategoryDTO dto : dtos) {
            ids.add(dto.getId());
        }
        return ids;
    }

    public Optional<CategoryDTO> findByName(String name) {
        log.info("Find category by name '{}'", name);

        String cql = "SELECT id FROM \"practical5Keyspace\".categories_id_by_name WHERE category_name = ?";

        try {
            PreparedStatement statement = connection.prepare(cql);
            BoundStatement bind = statement.bind(name);
            ResultSet resultSet = connection.execute(bind);

            if (resultSet.iterator().hasNext()) {
                Row row = resultSet.one();
                UUID categoryId = row.getUuid("category_id");

                log.info("Category '{}' found with ID '{}'", name, categoryId);
                return Optional.of(new CategoryDTO(categoryId, name));
            }

            log.warn("Category '{}' not found", name);
            return Optional.empty();
        } catch (Exception e) {
            log.error("An error occurred while finding category '{}': {}", name, e.getMessage());
            throw new RuntimeException("Failed to find category by name", e);
        }
    }

}
