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

    private final CqlSession connection;

    public ShopByCategoryDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<Pair<UUID, UUID>> insertBatch(List<ShopByCategoryDTO> dtos) {
        String cqlToCategories = "INSERT INTO \"practical5Keyspace\".shop_by_category (category_id, shop_id, category_quantity) VALUES (?,?,?)";
        PreparedStatement stmt = connection.prepare(cqlToCategories);
        for (ShopByCategoryDTO dto : dtos) {
            BoundStatement bind = stmt.bind(dto.getCategoryId(), dto.getShopId(), dto.getQuantity());
            connection.executeAsync(bind);
        }
        return retrieveIds(dtos);
    }

    public List<Pair<UUID, UUID>> insertInChunks(List<ShopByCategoryDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<Pair<UUID, UUID>> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<ShopByCategoryDTO> chunk = dtos.subList(i, end);
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private List<Pair<UUID, UUID>> retrieveIds(List<ShopByCategoryDTO> dtos) {
        List<Pair<UUID, UUID>> ids = new ArrayList<>();
        for (ShopByCategoryDTO dto : dtos) {
            ids.add(Pair.of(dto.getCategoryId(), dto.getShopId()));
        }
        return ids;
    }

    public Optional<StoreDTO> findStoreWithMostProductsByCategory(UUID categoryId) {

        String cqlToGetShopId = "SELECT shop_id FROM \"practical5Keyspace\".shop_by_category WHERE category_id = ?";
        String cqlTOGetShopAddres = "SELECT shop_address FROM \"practical5Keyspace\".shops WHERE shop_id = ?";
        Optional result;
        try {
            PreparedStatement stmtToGetShopId = connection.prepare(cqlToGetShopId);
            BoundStatement bind = stmtToGetShopId.bind(categoryId);
            ResultSet resultSet = connection.execute(bind);

            if (resultSet.iterator().hasNext()) {
                Row row = resultSet.one();
                UUID shopId = row.getUuid("shop_id");

                PreparedStatement stmtToGetShopAddress = connection.prepare(cqlTOGetShopAddres);
                BoundStatement bind1 = stmtToGetShopAddress.bind(shopId);
                ResultSet execute = connection.execute(bind1);
                if (execute.iterator().hasNext()) {
                    Row row1 = execute.one();
                    String address = row1.getString("shop_address");
                    return Optional.of(new StoreDTO(shopId, address));
                }
                log.info("Shop found with ID '{}'", shopId);
            }


            return Optional.empty();
        } catch (Exception e) {
            throw new RuntimeException("Failed to find category by name", e);
        }
    }
}
