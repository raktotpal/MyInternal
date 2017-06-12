package com.internal.spring.mvc.rest.webService.POJO;

public class Country {

  private String label;
  private String dimensionName;

  public Country() {

  }

  public Country(String label, String dimensionName) {
    super();
    this.setLabel(label);
    this.setDimensionName(dimensionName);
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public void setDimensionName(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

}