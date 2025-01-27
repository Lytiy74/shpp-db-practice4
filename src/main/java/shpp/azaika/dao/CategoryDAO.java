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

public class CategoryDAO implements DAO<CategoryDTO> {
    private static final Logger log = LoggerFactory.getLogger(CategoryDAO.class);
    private static final String INSERT_CATEGORY = "INSERT INTO \"practical5Keyspace\".categories (category_id, category_name) VALUES (?,?)";
    private static final String INSERT_CATEGORY_ID_BY_NAME = "INSERT INTO \"practical5Keyspace\".categories_id_by_name (category_name, category_id) VALUES (?,?)";
    private static final String SELECT_CATEGORY_BY_NAME = "SELECT category_id FROM \"practical5Keyspace\".categories_id_by_name WHERE category_name = ?";

    private final CqlSession connection;

    public CategoryDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<UUID> insertBatch(List<CategoryDTO> dtos) {
        PreparedStatement stmt = connection.prepare(INSERT_CATEGORY);
        PreparedStatement stmtIdByName = connection.prepare(INSERT_CATEGORY_ID_BY_NAME);

        dtos.forEach(dto -> {
            executeAsync(stmt.bind(dto.getId(), dto.getCategoryName()));
            executeAsync(stmtIdByName.bind(dto.getCategoryName(), dto.getId()));
        });

        return extractIds(dtos);
    }

    public List<UUID> insertInChunks(List<CategoryDTO> dtos, int chunkSize) {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < dtos.size(); i += chunkSize) {
            List<CategoryDTO> chunk = dtos.subList(i, Math.min(i + chunkSize, dtos.size()));
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private void executeAsync(BoundStatement boundStatement) {
        connection.executeAsync(boundStatement);
    }

    private List<UUID> extractIds(List<CategoryDTO> dtos) {
        List<UUID> ids = new ArrayList<>();
        dtos.forEach(dto -> ids.add(dto.getId()));
        return ids;
    }

    public Optional<CategoryDTO> findByName(String name) {
        log.info("Finding category by name '{}'.", name);

        try {
            PreparedStatement statement = connection.prepare(SELECT_CATEGORY_BY_NAME);
            BoundStatement bind = statement.bind(name);
            ResultSet resultSet = connection.execute(bind);

            if (resultSet.iterator().hasNext()) {
                Row row = resultSet.one();
                UUID categoryId = row.getUuid("category_id");

                log.info("Category '{}' found with ID '{}'.", name, categoryId);
                return Optional.of(new CategoryDTO(categoryId, name));
            }

            log.warn("Category '{}' not found.", name);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Error occurred while finding category '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to find category by name", e);
        }
    }
}
