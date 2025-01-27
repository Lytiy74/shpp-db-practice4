package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.ShopByCategoryDTO;
import shpp.azaika.dto.StoreDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ShopByCategoryDAO {
    private static final Logger log = LoggerFactory.getLogger(ShopByCategoryDAO.class);

    private static final String INSERT_SHOP_BY_CATEGORY =
            "INSERT INTO \"practical5Keyspace\".shop_by_category (category_id, shop_id, category_quantity) VALUES (?,?,?)";
    private static final String SELECT_SHOP_ID_BY_CATEGORY =
            "SELECT shop_id, category_quantity FROM \"practical5Keyspace\".shop_by_category WHERE category_id = ? LIMIT 1";
    private static final String SELECT_SHOP_ADDRESS =
            "SELECT shop_address FROM \"practical5Keyspace\".shops WHERE shop_id = ?";

    private final CqlSession connection;

    public ShopByCategoryDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<Pair<UUID, UUID>> insertBatch(List<ShopByCategoryDTO> dtos) {
        log.info("Inserting batch of {} shop-by-category entries.", dtos.size());

        PreparedStatement stmt = connection.prepare(INSERT_SHOP_BY_CATEGORY);
        dtos.forEach(dto -> executeAsync(stmt.bind(dto.getCategoryId(), dto.getShopId(), dto.getQuantity())));

        return extractIds(dtos);
    }

    public List<Pair<UUID, UUID>> insertInChunks(List<ShopByCategoryDTO> dtos, int chunkSize) {
        log.info("Inserting shop-by-category entries in chunks of size {}.", chunkSize);

        List<Pair<UUID, UUID>> ids = new ArrayList<>();
        for (int i = 0; i < dtos.size(); i += chunkSize) {
            List<ShopByCategoryDTO> chunk = dtos.subList(i, Math.min(i + chunkSize, dtos.size()));
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    public Optional<StoreDTO> findStoreWithMostProductsByCategory(UUID categoryId) {
        log.info("Finding store with most products for category ID '{}'.", categoryId);

        try {
            UUID shopId = executeQuery(SELECT_SHOP_ID_BY_CATEGORY, categoryId, "shop_id", UUID.class)
                    .orElse(null);

            if (shopId == null) {
                log.warn("No shop found for category ID '{}'.", categoryId);
                return Optional.empty();
            }
            String shopAddress = executeQuery(SELECT_SHOP_ADDRESS, shopId, "shop_address", String.class)
                    .orElse(null);

            if (shopAddress != null) {
                log.info("Found shop with ID '{}' and address '{}'.", shopId, shopAddress);
                return Optional.of(new StoreDTO(shopId, shopAddress));
            }

            log.warn("Shop address not found for shop ID '{}'.", shopId);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Error occurred while finding store: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to find store for category ID", e);
        }
    }

    private <T> Optional<T> executeQuery(String query, Object param, String columnName, Class<T> type) {
        try {
            PreparedStatement stmt = connection.prepare(query);
            BoundStatement bind = stmt.bind(param);
            ResultSet resultSet = connection.execute(bind);

            if (resultSet.iterator().hasNext()) {
                Row row = resultSet.one();
                return Optional.ofNullable(row.get(columnName, type));
            }
        } catch (Exception e) {
            log.error("Error executing query '{}': {}", query, e.getMessage(), e);
        }
        return Optional.empty();
    }

    private void executeAsync(BoundStatement boundStatement) {
        connection.executeAsync(boundStatement);
    }

    private List<Pair<UUID, UUID>> extractIds(List<ShopByCategoryDTO> dtos) {
        List<Pair<UUID, UUID>> ids = new ArrayList<>();
        dtos.forEach(dto -> ids.add(Pair.of(dto.getCategoryId(), dto.getShopId())));
        return ids;
    }
}
