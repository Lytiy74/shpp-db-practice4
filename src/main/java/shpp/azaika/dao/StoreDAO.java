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

public class StoreDAO {
    private static final Logger log = LoggerFactory.getLogger(StoreDAO.class);

    private final CqlSession connection;

    public StoreDAO(CqlSession connection) {
        this.connection = connection;
    }


    public List<UUID> insertBatch(List<StoreDTO> dtos) {
        String cql = "INSERT INTO \"practical5Keyspace\".shops (shop_id, shop_address) VALUES (?,?)";
        PreparedStatement stmt = connection.prepare(cql);
        for (StoreDTO dto : dtos) {
            BoundStatement bind = stmt.bind(dto.getId(), dto.getAddress());
            connection.executeAsync(bind);
        }
        return retrieveIds(dtos);
    }

    public List<UUID> insertInChunks(List<StoreDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<StoreDTO> chunk = dtos.subList(i, end);
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private List<UUID> retrieveIds(List<StoreDTO> dtos) {
        List<UUID> ids = new ArrayList<>();
        for (StoreDTO dto : dtos) {
            ids.add(dto.getId());
        }
        return ids;
    }
}
