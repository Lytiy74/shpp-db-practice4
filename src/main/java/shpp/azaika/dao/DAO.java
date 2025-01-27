package shpp.azaika.dao;

import java.util.List;
import java.util.UUID;

public interface DAO<T> {
    List<UUID> insertInChunks(List<T> entities, int chunkSize);
}
