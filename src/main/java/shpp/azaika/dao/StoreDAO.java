package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.StoreDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StoreDAO implements DAO<StoreDTO> {
    private static final Logger log = LoggerFactory.getLogger(StoreDAO.class);

    private static final String INSERT_STORE =
            "INSERT INTO \"practical5Keyspace\".shops (shop_id, shop_address) VALUES (?,?)";

    private final CqlSession connection;

    public StoreDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<UUID> insertBatch(List<StoreDTO> dtos) {
        log.info("Inserting batch of {} stores.", dtos.size());

        PreparedStatement stmt = connection.prepare(INSERT_STORE);
        dtos.forEach(dto -> executeAsync(stmt.bind(dto.getId(), dto.getAddress())));

        return extractIds(dtos);
    }

    public List<UUID> insertInChunks(List<StoreDTO> dtos, int chunkSize) {
        log.info("Inserting stores in chunks of size {}.", chunkSize);

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < dtos.size(); i += chunkSize) {
            List<StoreDTO> chunk = dtos.subList(i, Math.min(i + chunkSize, dtos.size()));
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private void executeAsync(BoundStatement boundStatement) {
        connection.executeAsync(boundStatement);
        log.debug("Executed async insert: {}", boundStatement.getPreparedStatement().getQuery());
    }

    private List<UUID> extractIds(List<StoreDTO> dtos) {
        List<UUID> ids = new ArrayList<>();
        dtos.forEach(dto -> ids.add(dto.getId()));
        return ids;
    }
}
