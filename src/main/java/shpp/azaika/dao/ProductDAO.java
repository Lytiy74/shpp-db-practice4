package shpp.azaika.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.dto.ProductDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ProductDAO {
    private static final Logger log = LoggerFactory.getLogger(ProductDAO.class);

    private final CqlSession connection;

    public ProductDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<UUID> insertBatch(List<ProductDTO> dtos) {
        String cql = "INSERT INTO \"practical5Keyspace\".products (product_id, product_name, category_id, product_price) VALUES (?,?,?,?)";
        PreparedStatement stmt = connection.prepare(cql);
        for (ProductDTO dto : dtos) {
            BoundStatement bind = stmt.bind(dto.getId(), dto.getName(), dto.getCategoryId(), dto.getPrice());
            connection.executeAsync(bind);
        }
        return retrieveIds(dtos);
    }

    public List<UUID> insertInChunks(List<ProductDTO> dtos, int chunkSize) {
        int total = dtos.size();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < total; i += chunkSize) {
            int end = Math.min(i + chunkSize, total);
            List<ProductDTO> chunk = dtos.subList(i, end);
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private List<UUID> retrieveIds(List<ProductDTO> dtos) {
        List<UUID> ids = new ArrayList<>();
        for (ProductDTO dto : dtos) {
            ids.add(dto.getId());
        }
        return ids;
    }
}
