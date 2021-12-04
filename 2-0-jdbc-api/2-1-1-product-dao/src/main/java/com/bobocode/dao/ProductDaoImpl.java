package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;
import com.bobocode.util.ExerciseNotCompletedException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

public class ProductDaoImpl implements ProductDao {

  private DataSource dataSource;
  private static final String FIND_ALL_PRODUCTS = "SELECT * FROM products;";
  private static final String FIND_ONE_PRODUCT = "SELECT * FROM products WHERE id = ?;";
  private static final String REMOVE_PRODUCT = "DELETE FROM products WHERE id = ?;";
  private static final String SAVE_PRODUCT = "INSERT INTO products (name, producer, price, expirationDate) VALUES (?, ?, ?, ?)";

  public ProductDaoImpl(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  private Product fillFieldsProduct(ResultSet result) {
    Product product = new Product();
    try {
      product.setId(result.getLong(1));
      product.setName(result.getString(2));
      product.setProducer(result.getString(3));
      product.setPrice(result.getBigDecimal(4));
      product.setExpirationDate(result.getDate(5).toLocalDate());
      product.setCreationTime(result.getTimestamp(6).toLocalDateTime());
    } catch (SQLException e) {
      throw new DaoOperationException("Error filling fields", e);
    }
    return product;
  }

  private Connection getConnection() {
    Connection connection;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      throw new DaoOperationException("Error establishing a connection with the database", e);
    }
    return connection;
  }

  private Statement getStatement(Connection connection) {
    Statement statement;
    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new DaoOperationException("Error getting a statement from the connection", e);
    }
    return statement;
  }

  private PreparedStatement getPrepareStatement(Connection connection, String query) {
    PreparedStatement preparedStatement;
    try {
      preparedStatement = connection.prepareStatement(query);
    } catch (SQLException e) {
      throw new DaoOperationException("Error getting a prepareStatement from the connection", e);
    }
    return preparedStatement;
  }


  private ResultSet getResultSet(Statement statement, String query) {
    ResultSet result;
    try {
      result = statement.executeQuery(query);
    } catch (SQLException e) {
      throw new DaoOperationException("Error getting a resultSet from the statement", e);
    }
    return result;
  }

  private ResultSet getResultSet(PreparedStatement statement, Long id) {
    ResultSet resultSet;
    try {
      statement.setLong(1, id);
      resultSet = statement.executeQuery();
    } catch (SQLException e) {
      throw new DaoOperationException("Error getting a resultSet from the prepareStatement", e);
    }
    return resultSet;
  }

  private boolean isHasNext(ResultSet resultSet) {
    boolean result;
    try {
      result = resultSet.next();
    } catch (SQLException e) {
      throw new DaoOperationException("Error check is resultSet has next", e);
    }
    return result;
  }

  private void closeConnectionStatement(Connection connection, Statement statement) {
    try {
      statement.close();
      connection.close();
    } catch (SQLException e) {
      throw new DaoOperationException("Error closing connection or statement", e);
    }
  }

  private void checkIsIdProductNull(Product product) {
    if (product.getId() == null) {
      throw new DaoOperationException("Product id cannot be null");
    }
  }

  private void checkIsIdProductValid(Product product) {
    if (product.getId() < 0) {
      throw new DaoOperationException("Product with id = " + product.getId() + " does not exist");
    }
  }

  ////////////////////////////////

//  @Override
//  public void save(Product product) {
//    Objects.requireNonNull(product);
//    try (Connection connection = dataSource.getConnection()) { // try-with-resources will automatically close connection
//      saveProduct(product, connection);
//    } catch (SQLException e) {
//      throw new DaoOperationException(String.format("Error saving product: %s", product), e);
//    }
//  }

//  private void saveProduct(Product product, Connection connection) throws SQLException {
//    PreparedStatement insertStatement = prepareInsertStatement(product, connection);
//    insertStatement.executeUpdate();
//    Long id = fetchGeneratedId(insertStatement);
//    product.setId(id);
//  }
//
//  private PreparedStatement prepareInsertStatement(Product product, Connection connection) {
//    try {
//      PreparedStatement insertStatement = connection.prepareStatement(INSERT_SQL,
//          PreparedStatement.RETURN_GENERATED_KEYS); // this parameter will configure query to ask db for a generated keys
//      fillProductStatement(product, insertStatement);
//      return insertStatement;
//    } catch (SQLException e) {
//      throw new DaoOperationException(String.format("Cannot prepare statement for product: %s", product), e);
//    }
//  }
//
//  private void fillProductStatement(Product product, PreparedStatement updateStatement) throws SQLException {
//    updateStatement.setString(1, product.getName());
//    updateStatement.setString(2, product.getProducer());
//    updateStatement.setBigDecimal(3, product.getPrice());
//    updateStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
//  }
//
//  private Long fetchGeneratedId(PreparedStatement insertStatement) throws SQLException {
//    // this method allows to retrieve IDs that were generated by the database during insert statement
//    ResultSet generatedKeys = insertStatement.getGeneratedKeys();
//    if (generatedKeys.next()) { // you need to call next() because cursor is located before the first row
//      return generatedKeys.getLong(1);
//    } else { // if next() returned false it means that database didn't return any IDs
//      throw new DaoOperationException("Can not obtain product ID");
//    }
//  }
  ////////////////////////////////////

  private void saveProduct(Product product, Connection connection) throws SQLException {
    var insertPreparedStatement = createPrepareStatementForInsert(connection, product);
    insertPreparedStatement.executeUpdate();
    Long id = getGeneratedId(insertPreparedStatement);
    product.setId(id);
  }

  private Long getGeneratedId(PreparedStatement preparedStatement) throws SQLException {
    ResultSet resultSet = preparedStatement.getGeneratedKeys();
    if (resultSet.next()) {
      return resultSet.getLong(1);
    } else {
      throw new DaoOperationException("sfsdfsf");
    }
  }

  private void fillPreparedStatement(PreparedStatement preparedStatement, Product product) {
    try {
      preparedStatement.setString(1, product.getName());
      preparedStatement.setString(2, product.getProducer());
      preparedStatement.setBigDecimal(3, product.getPrice());
      preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
    } catch (SQLException e) {
      throw new DaoOperationException("saaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", e);
    }
  }

  private PreparedStatement createPrepareStatementForInsert(Connection connection,
      Product product) {
    PreparedStatement preparedStatement;
    try {
      preparedStatement = connection.prepareStatement(SAVE_PRODUCT,
          PreparedStatement.RETURN_GENERATED_KEYS);
      fillPreparedStatement(preparedStatement, product);

    } catch (SQLException e) {
      throw new DaoOperationException("Error saving product: " + product, e);
    }
    return preparedStatement;
  }

  @Override
  public void save(Product product) {
//    throw new ExerciseNotCompletedException();// todo
    Objects.requireNonNull(product);
    try (Connection connection = dataSource.getConnection()) {
      saveProduct(product, connection);
    } catch (SQLException e) {
      throw new DaoOperationException("sdfdsfds", e);
    }


  }

  @Override
  public List<Product> findAll() {
//        throw new ExerciseNotCompletedException();// todo
    List<Product> productList = new ArrayList<>();
    var connection = getConnection();
    var statement = getStatement(connection);
    var result = getResultSet(statement, FIND_ALL_PRODUCTS);
    while (isHasNext(result)) {
      Product product = fillFieldsProduct(result);
      productList.add(product);
    }
    closeConnectionStatement(connection, statement);
    return productList;
  }

  @Override
  public Product findOne(Long id) {
//    throw new ExerciseNotCompletedException();// todo
    if (id < 0){
      throw new DaoOperationException("Product with id = " + id + " does not exist");
    }
    var product = new Product();
    var connection = getConnection();
    var statement = getPrepareStatement(connection, FIND_ONE_PRODUCT);
    ResultSet result = getResultSet(statement, id);
    isHasNext(result);
    product = fillFieldsProduct(result);
    closeConnectionStatement(connection, statement);
    return product;
  }

  @Override
  public void update(Product product) {
    throw new ExerciseNotCompletedException();// todo
  }

  @Override
  public void remove(Product product) {
//    throw new ExerciseNotCompletedException();// todo
    checkIsIdProductNull(product);
    checkIsIdProductValid(product);
    var connection = getConnection();
    var statement = getPrepareStatement(connection, REMOVE_PRODUCT);
    try {
      statement.setLong(1, product.getId());
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new DaoOperationException("sdf", e);
    }
    closeConnectionStatement(connection, statement);
  }
}
