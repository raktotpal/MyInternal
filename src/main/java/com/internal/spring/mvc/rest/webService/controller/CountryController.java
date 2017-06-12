package com.internal.spring.mvc.rest.webService.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.internal.spring.mvc.rest.webService.POJO.Country;

@RestController
/* @RequestMapping("/hello") */
public class CountryController {
  @RequestMapping(value = "/countries", method = RequestMethod.GET, headers = "Accept=application/json") public List<Country> getCountries() {
    List<Country> listOfCountries = new ArrayList<Country>();
    listOfCountries = createCountryList();
    return listOfCountries;
  }

  @RequestMapping(value = "/addStudent", method = RequestMethod.POST) public @ResponseBody List<Country> addStudent(
      @RequestBody List<Country> cnts) {

    return cnts;
  }

  // Utiliy method to create country list.
  public List<Country> createCountryList() {
    Country indiaCountry = new Country("1", "India");
    Country bhutanCountry = new Country("2", "Bhutan");
    Country chinaCountry = new Country("4", "China");
    Country nepalCountry = new Country("3", "Nepal");

    List<Country> listOfCountries = new ArrayList<Country>();
    listOfCountries.add(indiaCountry);
    listOfCountries.add(chinaCountry);
    listOfCountries.add(nepalCountry);
    listOfCountries.add(bhutanCountry);
    return listOfCountries;
  }
}