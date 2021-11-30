package com.bobocode.dao;

import com.bobocode.model.Product;
import com.bobocode.util.ExerciseNotCompletedException;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import javax.sql.DataSource;
import java.util.List;
import lombok.SneakyThrows;

public class ProductDaoImpl implements ProductDao {

  private DataSource dataSource;

  public ProductDaoImpl(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void save(Product product) {
    throw new ExerciseNotCompletedException();// todo
  }

  @SneakyThrows
  @Override
  public List<Product> findAll() {
//        throw new ExerciseNotCompletedException();// todo
    List<Product> productList = new ArrayList<>();
    try (var connection = dataSource.getConnection()) {
      try (var statement = connection.createStatement()) {
        var result = statement.executeQuery("SELECT * FROM products;");
        while (result.next()) {
          Product product = new Product();
          product.setId(result.getLong(1));
          product.setName(result.getString(2));
          product.setProducer(result.getString(3));
          product.setPrice(result.getBigDecimal(4));
          product.setExpirationDate(result.getDate(5).toLocalDate());
          product.setCreationTime(result.getTimestamp(6).toLocalDateTime());
          productList.add(product);
        }
      }
    }
    return productList;
  }

  @SneakyThrows
  @Override
  public Product findOne(Long id) {
//    throw new ExerciseNotCompletedException();// todo
    var product = new Product();
    product.setId(id);
    try (var connection = dataSource.getConnection()) {
      try (var statement = connection.prepareStatement("SELECT * FROM products WHERE id = (?)")) {
        statement.setLong(1, id);
        var result = statement.executeQuery();
        while (result.next()) {
          product.setName(result.getString(2));
          product.setProducer(result.getString(3));
          product.setPrice(result.getBigDecimal(4));
          product.setExpirationDate(result.getDate(5).toLocalDate());
          product.setCreationTime(result.getTimestamp(6).toLocalDateTime());
        }
      }
    }
    return product;
  }

  @Override
  public void update(Product product) {
    throw new ExerciseNotCompletedException();// todo
  }

  @Override
  public void remove(Product product) {
    throw new ExerciseNotCompletedException();// todo
  }

}
