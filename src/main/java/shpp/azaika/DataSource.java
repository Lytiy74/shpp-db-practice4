package shpp.azaika;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DataSource {
    private static DataSource instance;

    private HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;

    private DataSource() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties"));
        config.setJdbcUrl(properties.getProperty("jdbc.url"));
        config.setUsername(properties.getProperty("jdbc.username"));
        config.setPassword(properties.getProperty("jdbc.password"));
        config.setAutoCommit(false);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }
    public static DataSource getInstance() throws IOException {
        if(instance == null) instance = new DataSource();
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
