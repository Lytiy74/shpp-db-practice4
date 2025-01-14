package shpp.azaika.dao;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import shpp.azaika.dto.CategoryDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class CategoryDAOTest {

    @Mock
    Connection connectionMock;

    @Mock
    PreparedStatement stmt;

    @Mock
    ResultSet resultSet;

    @Captor
    ArgumentCaptor<String> captor;

    private CategoryDAO categoryDAO;
    private List<CategoryDTO> dtos;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        Mockito.when(connectionMock.prepareStatement(Mockito.anyString(), Mockito.eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(stmt);
        Mockito.when(connectionMock.prepareStatement(Mockito.anyString())).thenReturn(stmt);
        Mockito.when(stmt.getGeneratedKeys()).thenReturn(resultSet);

        categoryDAO = new CategoryDAO(connectionMock);
        dtos = List.of(
                new CategoryDTO("Category 1"),
                new CategoryDTO("Category 2")
        );
    }

    @Test
    @DisplayName("Test if list is inserted in batch")
    void insertBatchTest_insertionInBatch() throws SQLException {
        Mockito.when(resultSet.next()).thenReturn(false);

        categoryDAO.insertBatch(dtos);

        Mockito.verify(stmt, Mockito.times(dtos.size())).setString(Mockito.eq(1), captor.capture());
        List<String> capturedStrings = captor.getAllValues();

        Assertions.assertEquals("Category 1", capturedStrings.get(0));
        Assertions.assertEquals("Category 2", capturedStrings.get(1));
    }

    @Test
    @DisplayName("Test if insertBatch returns correct IDs")
    void insertBatchTest_returnsCorrectIds() throws SQLException {
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getShort(1)).thenReturn((short) 1, (short) 2);

        List<Short> ids = categoryDAO.insertBatch(dtos);

        Assertions.assertEquals(1, (short)ids.get(0));
        Assertions.assertEquals(2,(short) ids.get(1));
    }

    @Test
    @DisplayName("Test if empty list passed to insertBatch")
    void insertBatchTest_emptyList() throws SQLException {
        List<CategoryDTO> emptyList = new ArrayList<>();

        List<Short> ids = categoryDAO.insertBatch(emptyList);

        Assertions.assertTrue(ids.isEmpty());
        Mockito.verify(stmt, Mockito.never()).setString(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    @DisplayName("Test if list is inserted in chunks")
    void insertInChunksTest() throws SQLException {
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getShort(1)).thenReturn((short) 1, (short) 2);

        List<Short> ids = categoryDAO.insertInChunks(dtos, 1);

        Assertions.assertEquals(2, ids.size());
        Assertions.assertEquals(1,(short) ids.get(0));
        Assertions.assertEquals(2,(short) ids.get(1));
        Mockito.verify(stmt, Mockito.times(2)).executeBatch();
    }

    @Test
    @DisplayName("Test findByName when category exists")
    void findByNameTest_found() throws SQLException {
        Mockito.when(stmt.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getShort("id")).thenReturn((short) 1);
        Mockito.when(resultSet.getString("name")).thenReturn("Category 1");

        Optional<CategoryDTO> result = categoryDAO.findByName("Category 1");

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("Category 1", result.get().getCategoryName());
        Assertions.assertEquals(1, result.get().getId());
    }

    @Test
    @DisplayName("Test findByName when category does not exist")
    void findByNameTest_notFound() throws SQLException {
        Mockito.when(stmt.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(false);
        Optional<CategoryDTO> result = categoryDAO.findByName("Nonexistent Category");

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("Test findByName throws exception")
    void findByNameTest_exceptionThrown() throws SQLException {
        Mockito.when(stmt.executeQuery()).thenThrow(new SQLException("Database error"));

        SQLException exception = Assertions.assertThrows(SQLException.class, () -> categoryDAO.findByName("Category 1"));
        Assertions.assertEquals("Database error", exception.getMessage());
    }
}
