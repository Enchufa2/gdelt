source("functions.R")

country <- readLines("countries.txt")
country.fips <- countrycode::countrycode(country, "country.name", "fips")

filter_data(ActionGeo_CountryCode %in% country.fips)
