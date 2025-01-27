package shpp.azaika.util;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SchemaManager {
    private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

    private SchemaManager() {
    }

    private static final String KEYSPACE = "\"practical5Keyspace\"";

    private static final Map<String, String> TABLE_DEFINITIONS = Map.of(
            "categories", """
                    CREATE TABLE IF NOT EXISTS categories (
                        category_id UUID PRIMARY KEY,
                        category_name TEXT
                    )
                    """,
            "categories_id_by_name", """
                    CREATE TABLE IF NOT EXISTS categories_id_by_name (
                        category_name TEXT,
                        category_id UUID,
                        PRIMARY KEY (category_name)
                    )
                    """,
            "products", """
                    CREATE TABLE IF NOT EXISTS products (
                        product_id UUID PRIMARY KEY,
                        product_name TEXT,
                        category_id UUID,
                        product_price DECIMAL
                    )
                    """,
            "shop_by_category", """
                    CREATE TABLE IF NOT EXISTS shop_by_category (
                        category_id       UUID,
                        shop_id           UUID,
                        category_quantity INT,
                        PRIMARY KEY (category_id, category_quantity, shop_id)
                    ) WITH CLUSTERING ORDER BY (category_quantity DESC, shop_id ASC);
                    """,
            "shops", """
                    CREATE TABLE IF NOT EXISTS shops (
                        shop_id UUID PRIMARY KEY,
                        shop_address TEXT
                    )
                    """
    );

    public static void dropAndCreateTables() {
        try (CqlSession connection = CqlSession.builder().withKeyspace(KEYSPACE).build()) {
            TABLE_DEFINITIONS.keySet().forEach(table -> dropTable(connection, table));
            TABLE_DEFINITIONS.forEach((table, query) -> createTable(connection, query, table));
        } catch (Exception e) {
            log.error("Error while dropping or creating tables: ", e);
        }
    }

    private static void dropTable(CqlSession connection, String tableName) {
        String dropQuery = "DROP TABLE IF EXISTS " + tableName;
        executeQuery(connection, dropQuery, "Dropped table '%s'".formatted(tableName));
    }

    private static void createTable(CqlSession connection, String createQuery, String tableName) {
        executeQuery(connection, createQuery, "Created table '%s'".formatted(tableName));
    }

    private static void executeQuery(CqlSession connection, String query, String successMessage) {
        try {
            SimpleStatement stmt = SimpleStatement.newInstance(query);
            connection.execute(stmt);
            log.info(successMessage);
        } catch (Exception e) {
            log.error("Failed to execute query '{}': {}", query, e.getMessage());
        }
    }
}
