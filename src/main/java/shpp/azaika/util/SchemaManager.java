package shpp.azaika.util;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SchemaManager {
    private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

    private SchemaManager(){}

    private static final Map<String, String> TABLE_DEFINITIONS = Map.of(
            "categories", """
                    CREATE TABLE IF NOT EXISTS "practical5Keyspace".categories (
                        category_id UUID PRIMARY KEY,
                        category_name TEXT
                    )
                    """,
            "categories_id_by_name", """
                    CREATE TABLE IF NOT EXISTS "practical5Keyspace".categories_id_by_name (
                        category_name TEXT,
                        category_id UUID,
                        PRIMARY KEY (category_name, category_id)
                    )
                    """,
            "products", """
                    CREATE TABLE IF NOT EXISTS "practical5Keyspace".products (
                        product_id UUID PRIMARY KEY,
                        product_name TEXT,
                        category_id UUID,
                        product_price DECIMAL
                    )
                    """,
            "shop_by_category", """
                    CREATE TABLE IF NOT EXISTS "practical5Keyspace".shop_by_category (
                        category_id UUID,
                        shop_id UUID,
                        category_quantity INT,
                        PRIMARY KEY (category_id, shop_id)
                    )
                    """,
            "shops", """
                    CREATE TABLE IF NOT EXISTS "practical5Keyspace".shops (
                        shop_id UUID PRIMARY KEY,
                        shop_address TEXT
                    )
                    """
    );

    public static void dropAndCreateTables() {
        try (CqlSession connection = CqlSession.builder().build()) {
            TABLE_DEFINITIONS.keySet().forEach(table -> dropTable(connection, "\"practical5Keyspace\"." + table));
            TABLE_DEFINITIONS.forEach((table, query) -> createTable(connection, query, table));
        } catch (Exception e) {
            log.error("Error while dropping or creating tables: ", e);
        }
    }

    private static void dropTable(CqlSession connection, String tableName) {
        String dropQuery = "DROP TABLE IF EXISTS " + tableName;
        try {
            SimpleStatement stmt = SimpleStatement.newInstance(dropQuery);
            connection.execute(stmt);
            log.info("Dropped table '{}'", tableName);
        } catch (Exception e) {
            log.error("Failed to drop table '{}': {}", tableName, e.getMessage());
        }
    }


    private static void createTable(CqlSession connection, String createQuery, String tableName) {
        try {
            SimpleStatement stmt = SimpleStatement.newInstance(createQuery);
            connection.execute(stmt);
            log.info("Created table '{}'", tableName);
        } catch (Exception e) {
            log.error("Failed to create table '{}': {}", tableName, e.getMessage());
        }
    }
}
