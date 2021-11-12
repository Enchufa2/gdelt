.events.url <- "http://data.gdeltproject.org/events"
.header.url <- "https://www.gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt"
.data.path <- "data"
.cache.path <- file.path(.data.path, "cache")
.results.path <- file.path(.data.path, "results")

.cnames <- names(read.delim(.header.url))
.ctypes <- paste0(
  data = "iiiin",
  actor1 = "cccccccccc",
  actor2 = "cccccccccc",
  action = "lccciniiin",
  geo1 = "icccnnc",
  geo2 = "icccnnc",
  geo = "icccnnc",
  mgmt = "ic"
)

.get_file <- function(src, dst, md5sum, retries=3) {
  if (file.exists(dst))
    return(TRUE)
  rt <- 0; repeat {
    download.file(src, dst, quiet=TRUE)
    if (tools::md5sum(dst) == md5sum)
      break
    if ((rt <- rt + 1) == retries) {
      warning("Max. retries reached for file ", basename(dst))
      break
    }
  }
  file.exists(dst)
}

.reader <- coro::generator(function(years=NULL, cache=TRUE, retries=3) {
  dir.create(.cache.path, showWarnings=FALSE, recursive=TRUE)

  files <- vroom::vroom(
    file.path(.events.url, "md5sums"),
    delim="  ", col_names=c("md5sum", "name"), col_types="cc")[-1, ]
  files$year <- as.integer(substr(files$name, 1, 4))

  if (is.null(years))
    years <- unique(files$year)

  for (i in years) {
    message("Processing ", i, "...")
    files.year <- subset(files, year == i)

    append <- FALSE
    for (j in seq_len(nrow(files.year))) {
      name <- tools::file_path_sans_ext(files.year$name[j])
      if (!grepl("export", files.year$name[j]))
        name <- paste0(name, ".csv")

      src <- file.path(.events.url, files.year$name[j])
      dst <- file.path(.cache.path, files.year$name[j])
      if (!.get_file(src, dst, files.year$md5sum[j], retries))
        next

      df <- vroom::vroom(unz(dst, name), col_names=.cnames, col_types=.ctypes)
      coro::yield(list(df=df, append=append, year=i))

      append <- TRUE
      if (!cache) unlink(dst)
    }
  }
})

gdelt_filter <- function(expr, years=NULL, cache=TRUE, retries=3) {
  dir.create(.results.path, showWarnings=FALSE, recursive=TRUE)
  expr <- substitute(expr)

  coro::loop(for (obj in .reader(years, cache, retries)) {
    df <- subset(obj$df, eval(expr))
    res <- file.path(.results.path, paste0(obj$year, ".csv.gz"))
    vroom::vroom_write(df, res, append=obj$append)
  })
}

gdelt_aggregate <- function(...) {
  files <- dir(.results.path, full.names=TRUE)
  stopifnot(length(files) > 0)
  vars <- as.character(match.call()[-1L])

  df <- dplyr::bind_rows(lapply(files, vroom::vroom, col_types=.ctypes))
  df <- dplyr::group_by(df, MonthYear)
  for (var in vars) df <- dplyr::group_by(df, .data[[var]], .add=TRUE)

  df <- dplyr::summarise(
    df, .groups="drop",
    n_events = dplyr::n(),
    avg_importance = sum(IsRootEvent) / n_events,
    avg_stability = sum(GoldsteinScale) / n_events,
    avg_context = sum(AvgTone) / n_events,
    total_impact = sum(NumMentions)
  )

  df <- dplyr::mutate(df, MonthYear = lubridate::ym(MonthYear))
  df <- dplyr::mutate(df, Month = as.integer(lubridate::month(MonthYear)))
  df <- dplyr::mutate(df, Year = as.integer(lubridate::year(MonthYear)))
  df <- dplyr::select(df, -MonthYear)
  df <- dplyr::relocate(df, Year, Month)
  dplyr::filter(df, Year >= 1979)
}
