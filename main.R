source("functions.R")

country <- readLines("countries.txt")
country.fips <- countrycode::countrycode(country, "country.name", "fips")
event.root <- as.character(c(14, 17))

gdelt_filter(ActionGeo_CountryCode %in% country.fips & EventRootCode %in% event.root)

df <- gdelt_aggregate(ActionGeo_CountryCode, EventBaseCode)
df <- tidyr::complete(
  df, Year, Month, ActionGeo_CountryCode, EventBaseCode, fill=list(n_events=0))
vroom::vroom_write(df, "data/results/gdelt_latinamerican_14_17.csv.gz")
