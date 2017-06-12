package com.internal.java.simple;

import java.util.HashMap;
import java.util.Map;

public class SimpleTest {
  public static int hh = 1;

  public static void main(String[] args) {
    Map<String, String> attr = getMap("x");

    Map<String, String> attrAll = new HashMap<String, String>();
    attrAll.put("x", "v3");

    for (String x : attr.keySet()) {
      if (attrAll.containsKey(x)) {
        attrAll.put(x + "_" + hh, attr.get(x));
      } else {
        attrAll.put(x, attr.get(x));
      }
      hh++;
    }

    System.out.println(attrAll);
  }

  public static Map<String, String> getMap(String key) {
    Map<String, String> attr = new HashMap<String, String>();
    attr.put("x", "v1");

    if (attr.containsKey(key)) {
      attr.put("x" + "_" + hh, "v2");
    } else {
      attr.put("x", "v3");
    }
    hh++;

    System.out.println("-->> " + attr);
    return attr;
  }
}