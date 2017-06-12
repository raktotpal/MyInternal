package com.internal.spark.stockExercise;

import java.io.Serializable;

public class csvReader implements Serializable {
  private static final long serialVersionUID = 1L;
  private String _name;
  private int _age;

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    _name = name;
  }

  public int getAge() {
    return _age;
  }

  public void setAge(int age) {
    _age = age;
  }
}