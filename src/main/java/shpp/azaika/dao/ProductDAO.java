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
    private static final String INSERT_PRODUCT = "INSERT INTO \"practical5Keyspace\".products (product_id, product_name, category_id, product_price) VALUES (?,?,?,?)";

    private final CqlSession connection;

    public ProductDAO(CqlSession connection) {
        this.connection = connection;
    }

    public List<UUID> insertBatch(List<ProductDTO> products) {
        log.info("Inserting batch of {} products.", products.size());

        PreparedStatement stmt = connection.prepare(INSERT_PRODUCT);
        products.forEach(product -> executeAsync(stmt.bind(
                product.getId(),
                product.getName(),
                product.getCategoryId(),
                product.getPrice()
        )));

        return extractIds(products);
    }

    public List<UUID> insertInChunks(List<ProductDTO> products, int chunkSize) {
        log.info("Inserting products in chunks of size {}.", chunkSize);

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < products.size(); i += chunkSize) {
            List<ProductDTO> chunk = products.subList(i, Math.min(i + chunkSize, products.size()));
            ids.addAll(insertBatch(chunk));
        }
        return ids;
    }

    private void executeAsync(BoundStatement boundStatement) {
        connection.executeAsync(boundStatement);
    }

    private List<UUID> extractIds(List<ProductDTO> products) {
        List<UUID> ids = new ArrayList<>();
        products.forEach(product -> ids.add(product.getId()));
        return ids;
    }
}
