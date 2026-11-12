#' Read an HMISserve input
#'
#' Every input HMISserve reads from S3 goes through this function, so a run can
#' be pointed at a frozen snapshot instead of live data (roadmap #14, issue #17).
#'
#' With `HMISSERVE_INPUT_PREFIX` unset, this calls the same HMISdata loader with
#' the same bucket and folder the call sites used before, so behavior is
#' unchanged, including returning `NULL` when a read fails.
#'
#' With `HMISSERVE_INPUT_PREFIX` set (e.g. `_snapshots/2026-11-11`), every read
#' comes from `s3://shiny-data-cohhio/<prefix>/<subfolder>/`:
#'
#' | kind           | live source                     | snapshot subfolder |
#' |----------------|---------------------------------|--------------------|
#' | `hmis_parquet` | `hud.csv-daily/hmis_output`     | `hmis_output/`     |
#' | `hmis_csv`     | `hud.csv-daily/hmis_files`      | `hmis_files/`      |
#' | `looker`       | `looker-daily`                  | `looker/`          |
#' | `output`       | `shiny-data-cohhio/$DATA_ENV`   | `RME/`             |
#'
#' Set `HMISSERVE_LOG_READS=1` to log the bucket and key (or Looker prefix) of
#' every read. Temporary: used to check that snapshot runs make no live reads.
#'
#' @param name File name (`"Client.parquet"`, `"Organization.csv"`) or, for
#'   `kind = "looker"`, the Look name (`"Client_Offer"`).
#' @param kind Which input source the file comes from. See table above.
#' @param col_types Column spec passed to `HMISdata::load_hmis_csv()` or
#'   `HMISdata::load_looker_data()`. Ignored for parquet kinds.
#'
#' @return A data frame, or `NULL` if the read failed.
#' @export
hs_read <- function(name,
                    kind = c("hmis_parquet", "hmis_csv", "looker", "output"),
                    col_types = NULL) {
  kind <- match.arg(kind)
  src <- hs_source(kind)
  hs_log_read(kind, src, name)

  switch(
    kind,
    hmis_parquet = ,
    output = HMISdata::load_hmis_parquet(name, bucket = src$bucket, folder = src$folder),
    hmis_csv = HMISdata::load_hmis_csv(name, bucket = src$bucket, folder = src$folder,
                                       col_types = col_types),
    looker = HMISdata::load_looker_data(bucket = src$bucket, prefix = src$folder,
                                        filename = name, col_types = col_types)
  )
}

#' Resolve the bucket and folder for an input kind
#'
#' @inheritParams hs_read
#' @return `list(bucket, folder)`. `folder` is `NULL` for live Looker reads,
#'   which list the whole `looker-daily` bucket.
#' @keywords internal
#' @noRd
hs_source <- function(kind = c("hmis_parquet", "hmis_csv", "looker", "output")) {
  kind <- match.arg(kind)
  prefix <- Sys.getenv("HMISSERVE_INPUT_PREFIX", unset = "")

  if (!nzchar(prefix)) {
    return(switch(
      kind,
      hmis_parquet = list(bucket = "hud.csv-daily", folder = "hmis_output"),
      hmis_csv     = list(bucket = "hud.csv-daily", folder = "hmis_files"),
      looker       = list(bucket = "looker-daily", folder = NULL),
      output       = list(bucket = "shiny-data-cohhio",
                          folder = Sys.getenv("DATA_ENV", unset = "RME"))
    ))
  }

  prefix <- sub("^/+", "", sub("/+$", "", prefix))
  subfolder <- switch(
    kind,
    hmis_parquet = "hmis_output",
    hmis_csv     = "hmis_files",
    looker       = "looker",
    output       = "RME"
  )
  folder <- paste(prefix, subfolder, sep = "/")
  # Looker reads list by prefix; the trailing slash keeps "looker" from also
  # matching a sibling like "looker_old".
  if (kind == "looker") folder <- paste0(folder, "/")

  list(bucket = "shiny-data-cohhio", folder = folder)
}

hs_log_read <- function(kind, src, name) {
  if (!identical(Sys.getenv("HMISSERVE_LOG_READS"), "1")) return(invisible(NULL))
  key <- if (kind == "looker") {
    paste0(if (is.null(src$folder)) "" else src$folder, "<latest ", name, ">")
  } else {
    paste(src$folder, name, sep = "/")
  }
  uri <- paste0("s3://", src$bucket, "/", key)
  logger::log_info("hs_read: {uri}")
  invisible(uri)
}