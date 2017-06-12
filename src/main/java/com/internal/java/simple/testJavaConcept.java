package com.internal.java.simple;

public class testJavaConcept {

  static {
    System.out.println("STATIC");
  }

  public static void main(String[] args) {
    System.out.println("First Line.");

    Normal obj = new Normal();

    System.out.println(obj.getMessgae());
  }
}

class Normal {
  public String getMessgae() {
    return "INSIDE method.";
  }

  {
    System.out.println("NON-STATIC");
  }
}