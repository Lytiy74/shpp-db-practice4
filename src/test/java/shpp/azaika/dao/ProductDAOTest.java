package shpp.azaika.dao;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import shpp.azaika.dto.ProductDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

class ProductDAOTest {

    @Mock
    Connection connectionMock;

    @Mock
    PreparedStatement stmt;

    @Mock
    ResultSet resultSet;

    @Captor
    ArgumentCaptor<String> captor;

    private ProductDAO productDAO;
    private List<ProductDTO> dtos;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        Mockito.when(connectionMock.prepareStatement(Mockito.anyString(), Mockito.eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(stmt);
        Mockito.when(connectionMock.prepareStatement(Mockito.anyString())).thenReturn(stmt);
        Mockito.when(stmt.getGeneratedKeys()).thenReturn(resultSet);

        productDAO = new ProductDAO(connectionMock);
        dtos = List.of(
                new ProductDTO((short) 1,"Product 1", 10.0),
                new ProductDTO((short) 2,"Product 2", 20.0)
        );
    }

    @Test
    @DisplayName("Test if list is inserted in batch")
    void insertBatchTest_insertionInBatch() throws SQLException {
        Mockito.when(resultSet.next()).thenReturn(false);

        productDAO.insertBatch(dtos);

        Mockito.verify(stmt, Mockito.times(dtos.size())).setString(Mockito.eq(1), captor.capture());
        List<String> capturedStrings = captor.getAllValues();

        Assertions.assertEquals("Product 1", capturedStrings.get(0));
        Assertions.assertEquals("Product 2", capturedStrings.get(1));
    }

    @Test
    @DisplayName("Test if insertBatch returns correct IDs")
    void insertBatchTest_returnsCorrectIds() throws SQLException {
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getShort(1)).thenReturn((short) 1, (short) 2);

        List<Short> ids = productDAO.insertBatch(dtos);

        Assertions.assertEquals(1, (short)ids.get(0));
        Assertions.assertEquals(2,(short) ids.get(1));
    }

    @Test
    @DisplayName("Test if empty list passed to insertBatch")
    void insertBatchTest_emptyList() throws SQLException {
        List<ProductDTO> emptyList = new ArrayList<>();

        List<Short> ids = productDAO.insertBatch(emptyList);

        Assertions.assertTrue(ids.isEmpty());
        Mockito.verify(stmt, Mockito.never()).setString(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    @DisplayName("Test if list is inserted in chunks")
    void insertInChunksTest() throws SQLException {
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getShort(1)).thenReturn((short) 1, (short) 2);

        List<Short> ids = productDAO.insertInChunks(dtos, 1);

        Assertions.assertEquals(2, ids.size());
        Assertions.assertEquals(1,(short) ids.get(0));
        Assertions.assertEquals(2,(short) ids.get(1));
        Mockito.verify(stmt, Mockito.times(2)).executeBatch();
    }
}
