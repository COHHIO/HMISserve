# dev/build_contract.R
#
# Builds the output contract for HMISserve (roadmap issue #16, HS-03):
#   inst/contracts/outputs.yml
#
# For every file HMISserve publishes to s3://shiny-data-cohhio/<DATA_ENV>/ the
# contract records: name, format, producer function, source call site, arrow
# schema, whether the file may be empty, key columns, and the RminorElevated
# modules that read it. It also lists orphans (in S3, nothing here produces
# them) and unread files (in S3, nothing in RminorElevated reads them), with
# last-modified dates. Nothing is deleted.
#
# The contract holds structure only. No data values are written.
#
# Usage (needs AWS credentials for the bucket, and a checkout of RminorElevated):
#   RME_PATH=~/code/RminorElevated Rscript dev/build_contract.R
#   DATA_ENV=RME is the default production folder.
#
# Reviewed entries: schemas built from one day's data can miss columns that are
# all-NA that day. Columns like that are listed under `review_columns`. After
# checking an entry by hand, set `reviewed: true`. Reruns then keep that entry's
# schema, may_be_empty and keys, and only warn when S3 has drifted from them.

suppressPackageStartupMessages({
  library(arrow)
})
for (pkg in c("yaml", "aws.s3", "cli")) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop("Package '", pkg, "' is required for dev/build_contract.R", call. = FALSE)
  }
}

BUCKET  <- "shiny-data-cohhio"
REGION  <- "us-east-2"
FOLDER  <- Sys.getenv("DATA_ENV", unset = "RME")
OUT     <- file.path("inst", "contracts", "outputs.yml")
RME     <- Sys.getenv("RME_PATH", unset = "")

# Names RminorElevated gets from somewhere other than this folder
# (see R/app_data.R in RminorElevated: load_local_data() and DEFERRED_LOADERS).
RME_NON_FOLDER <- c("Regions", "rm_dates", "program_lookup", "co_clients_served")

# ID-like columns tried as keys. Smallest unique combination (1-2 columns) wins.
KEY_CANDIDATES <- c("PersonalID", "EnrollmentID", "HouseholdID", "ProjectID",
                    "OrganizationID", "ReferralID", "ClientID")

# ---- 1. Find upload call sites in R/ ----------------------------------------

#' Scan R/*.R for upload_hmis_data() call sites.
#' Returns one row per call site: source file, order within the file, the
#' enclosing top-level function (the producer), and the literal file_name if
#' there is one.
find_call_sites <- function(r_dir = "R") {
  files <- sort(list.files(r_dir, pattern = "\\.R$", full.names = TRUE))
  rows <- list()
  for (f in files) {
    lines <- readLines(f, warn = FALSE)
    hits <- grep("upload_hmis_data\\(", lines)
    hits <- hits[!grepl("^\\s*#", lines[hits])]
    fn_lines <- grep("^[A-Za-z_.][A-Za-z0-9_.]*\\s*(<-|=)\\s*function", lines)
    for (i in seq_along(hits)) {
      h <- hits[i]
      prior <- fn_lines[fn_lines < h]
      producer <- if (length(prior)) {
        sub("\\s*(<-|=).*$", "", lines[max(prior)])
      } else NA_character_
      window <- lines[h:min(h + 8, length(lines))]
      lit <- regmatches(
        window,
        regexpr("file_name\\s*=\\s*\"[^\"]+\"", window)
      )
      lit <- if (length(lit)) sub(".*\"([^\"]+)\"", "\\1", lit[1]) else NA_character_
      fmt <- regmatches(window, regexpr("format\\s*=\\s*\"[a-z]+\"", window))
      fmt <- if (length(fmt)) sub(".*\"([a-z]+)\"", "\\1", fmt[1]) else "parquet"
      rows[[length(rows) + 1]] <- data.frame(
        source = f, call_site = i, producer = producer,
        file_name = lit, format = fmt, stringsAsFactors = FALSE
      )
    }
  }
  do.call(rbind, rows)
}

# Call sites whose file names are built at run time. The names are fixed by the
# code, so they are listed here and checked against S3 further down.
# data_quality.R: names of the `dq_data_files` list.
# project_evaluation*.R: paste0("pe_", snakecase::to_snake_case(<column>)) plus
# the two summary tables.
PE_PARTS <- c("scored_at_ph_entry", "return_to_homelessness",
              "homeless_history_index", "increase_income",
              "increase_earned_income", "length_of_stay", "res_prior",
              "benefits_at_exit", "exits_to_ph", "entries_no_income")

DYNAMIC <- list(
  "R/data_quality.R" = c("dq_past_year", "dq_overlaps", "dq_providers_df",
                         "dq_eligibility_detail", "dq_for_pe", "dq_main"),
  "R/project_evaluation.R" = c(paste0("pe_", PE_PARTS),
                               "pe_summary_final_scoring",
                               "pe_summary_validation"),
  "R/project_evaluation_mahoning.R" = c(
    paste0("pe_", setdiff(PE_PARTS, "length_of_stay"), "_mahoning"),
    "pe_summary_final_scoring_mahoning",
    "pe_summary_validation_mahoning"
  )
)

build_registry <- function() {
  cs <- find_call_sites()
  out <- list()
  for (i in seq_len(nrow(cs))) {
    r <- cs[i, ]
    if (!is.na(r$file_name)) {
      names_i <- sub("\\.(parquet|rds|csv|feather)$", "", r$file_name)
      dyn <- FALSE
    } else {
      names_i <- DYNAMIC[[r$source]]
      if (is.null(names_i)) {
        stop("Call site in ", r$source, " builds file_name at run time and is ",
             "not listed in DYNAMIC. Add it.", call. = FALSE)
      }
      dyn <- TRUE
    }
    for (nm in names_i) {
      out[[length(out) + 1]] <- list(
        name = nm, format = r$format, producer = r$producer,
        source = r$source, call_site = r$call_site, dynamic = dyn
      )
    }
  }
  # Uploaded by the container entry point, outside R/. Not part of the
  # R/ call-site test.
  out[[length(out) + 1]] <- list(
    name = "guidance", format = "parquet",
    producer = "HMISserve::get_guidance() via aws/process_hmis_serve_data.R",
    source = "aws/process_hmis_serve_data.R", call_site = 1L, dynamic = FALSE
  )
  out
}

# ---- 2. Describe a file's structure (no values) ------------------------------

describe_table <- function(tbl) {
  sch <- tbl$schema
  types <- vapply(sch$fields, function(f) f$type$ToString(), character(1))
  cols  <- vapply(sch$fields, function(f) f$name, character(1))
  n <- tbl$num_rows
  all_na <- vapply(seq_along(cols), function(j) {
    n > 0 && tbl$column(j - 1L)$null_count == n
  }, logical(1))
  list(
    schema = stats::setNames(as.list(types), cols),
    n_rows_zero = n == 0,
    review_columns = unname(cols[all_na | types == "null"])
  )
}

find_keys <- function(tbl) {
  cols <- intersect(KEY_CANDIDATES, names(tbl))
  if (!length(cols) || tbl$num_rows == 0) return(character())
  df <- as.data.frame(
    stats::setNames(
      lapply(cols, function(cl) as.vector(tbl$GetColumnByName(cl))),
      cols
    ),
    stringsAsFactors = FALSE
  )
  combos <- c(lapply(cols, identity),
              if (length(cols) > 1) utils::combn(cols, 2, simplify = FALSE))
  for (cmb in combos) {
    if (!anyDuplicated(df[, cmb, drop = FALSE]) &&
        !anyNA(df[, cmb, drop = FALSE])) {
      return(cmb)
    }
  }
  character()
}

#' dq_summary.rds is a list of windows, each a list of pieces. Record names and
#' the schema of every data frame, and only the class of anything else.
describe_rds <- function(x) {
  if (is.data.frame(x)) {
    return(list(
      type = "data.frame",
      schema = stats::setNames(
        lapply(x, function(col) class(col)[1]), names(x)
      )
    ))
  }
  if (is.list(x) && !inherits(x, c("gg", "ggplot"))) {
    return(list(type = "list",
                elements = lapply(x, describe_rds)))
  }
  list(type = paste(class(x), collapse = "/"))
}

fetch_to_tmp <- function(key) {
  tmp <- tempfile(fileext = paste0(".", tools::file_ext(key)))
  ok <- tryCatch({
    aws.s3::save_object(object = key, bucket = BUCKET, file = tmp,
                        region = REGION)
    TRUE
  }, error = function(e) {
    cli::cli_alert_warning("Could not fetch {key}: {conditionMessage(e)}")
    FALSE
  })
  if (ok) tmp else NA_character_
}

# ---- 3. RminorElevated consumers ---------------------------------------------

find_consumers <- function(rme_path) {
  if (!nzchar(rme_path) || !dir.exists(file.path(rme_path, "R"))) return(NULL)
  files <- list.files(file.path(rme_path, "R"), pattern = "\\.R$",
                      full.names = TRUE)
  hits <- list()
  for (f in files) {
    txt <- paste(readLines(f, warn = FALSE), collapse = "\n")
    m <- gregexpr("get_app_data\\(\\s*\"[^\"]+\"\\s*\\)", txt)
    nm <- regmatches(txt, m)[[1]]
    nm <- unique(sub(".*\"([^\"]+)\".*", "\\1", nm))
    nm <- nm[!grepl("[<>]", nm)] # skips the get_app_data("<name>") doc example
    for (n in nm) hits[[n]] <- c(hits[[n]], basename(f))
  }
  lapply(hits, function(v) sort(unique(v)))
}

# ---- 4. Build ----------------------------------------------------------------

main <- function() {
  if (!file.exists("DESCRIPTION")) stop("Run from the package root.", call. = FALSE)
  registry  <- build_registry()
  consumers <- find_consumers(RME)
  previous  <- if (file.exists(OUT)) yaml::read_yaml(OUT) else NULL
  prev_by_name <- if (!is.null(previous)) {
    stats::setNames(previous$files, vapply(previous$files, `[[`, "", "name"))
  } else list()

  if (is.null(consumers)) {
    cli::cli_alert_warning(
      "RME_PATH not set or not a checkout: keeping consumers from the existing contract."
    )
  }

  s3 <- aws.s3::get_bucket_df(bucket = BUCKET, prefix = paste0(FOLDER, "/"),
                              region = REGION, max = Inf)
  s3$file <- basename(s3$Key)
  s3 <- s3[nzchar(s3$file), , drop = FALSE]
  s3$name <- tools::file_path_sans_ext(s3$file)

  files <- lapply(registry, function(e) {
    key <- paste0(FOLDER, "/", e$name, ".", e$format)
    prev <- prev_by_name[[e$name]]
    entry <- list(
      name = e$name, format = e$format, producer = e$producer,
      source = e$source, call_site = e$call_site, dynamic = e$dynamic,
      reviewed = isTRUE(prev$reviewed)
    )
    entry$consumers <- if (!is.null(consumers)) {
      as.list(consumers[[e$name]] %||% character())
    } else prev$consumers %||% list()

    in_s3 <- e$name %in% s3$name[s3$file == paste0(e$name, ".", e$format)]
    if (!in_s3) {
      cli::cli_alert_warning("{e$name}.{e$format} is not in s3://{BUCKET}/{FOLDER}/")
      entry$in_s3 <- FALSE
      if (!is.null(prev)) {
        entry[c("schema", "may_be_empty", "keys", "review_columns")] <-
          prev[c("schema", "may_be_empty", "keys", "review_columns")]
      }
      return(entry)
    }
    entry$in_s3 <- TRUE

    tmp <- fetch_to_tmp(key)
    if (is.na(tmp)) return(entry)
    on.exit(unlink(tmp), add = TRUE)

    if (e$format == "rds") {
      entry$structure <- describe_rds(readRDS(tmp))
      entry$may_be_empty <- prev$may_be_empty %||% FALSE
      return(entry)
    }

    tbl <- arrow::read_parquet(tmp, as_data_frame = FALSE)
    d   <- describe_table(tbl)
    fresh <- list(
      schema = d$schema,
      may_be_empty = isTRUE(prev$may_be_empty) || d$n_rows_zero,
      keys = as.list(find_keys(tbl)),
      review_columns = as.list(d$review_columns)
    )
    if (isTRUE(prev$reviewed)) {
      if (!identical(prev$schema, fresh$schema)) {
        cli::cli_alert_warning(
          "{e$name}: S3 schema differs from the reviewed contract entry. Kept the reviewed one."
        )
      }
      fresh[c("schema", "may_be_empty", "keys")] <-
        prev[c("schema", "may_be_empty", "keys")]
    }
    c(entry, fresh)
  })

  produced <- vapply(files, `[[`, "", "name")
  produced_files <- paste0(produced, ".",
                           vapply(files, `[[`, "", "format"))

  read_names <- if (!is.null(consumers)) names(consumers) else
    unique(unlist(lapply(files, function(f) if (length(f$consumers)) f$name)))

  fmt_time <- function(x) format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%d")
  s3_data <- s3[tools::file_ext(s3$file) %in% c("parquet", "rds", "csv", "feather"), ]

  orphans <- s3_data[!(s3_data$file %in% produced_files), ]
  unread  <- s3_data[!(s3_data$name %in% read_names), ]
  as_rows <- function(df) lapply(seq_len(nrow(df)), function(i) {
    list(file = df$file[i], last_modified = fmt_time(df$LastModified[i]))
  })

  # Names RminorElevated reads that nothing here produces and that do not come
  # from another source.
  consumed_not_produced <- sort(setdiff(read_names, c(produced, RME_NON_FOLDER)))

  contract <- list(
    meta = list(
      bucket = BUCKET,
      folder = FOLDER,
      generated = format(Sys.Date()),
      note = paste(
        "Structure only; no data values. Set `reviewed: true` on an entry after",
        "checking its types by hand. Reruns keep reviewed schemas.",
        "RminorElevated loads every .rds/.parquet in the folder at boot, so",
        "unread files are still downloaded by the app."
      )
    ),
    files = files,
    orphans = as_rows(orphans),
    unread = as_rows(unread),
    consumed_not_produced = as.list(consumed_not_produced)
  )

  dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)
  # true/false, not yes/no: YAML 1.2 parsers read yes/no as strings.
  yaml::write_yaml(contract, OUT, handlers = list(logical = yaml::verbatim_logical))
  cli::cli_alert_success("Wrote {OUT} ({length(files)} entries, {nrow(orphans)} orphans, {nrow(unread)} unread)")
  if (length(consumed_not_produced)) {
    cli::cli_alert_warning(
      "RminorElevated reads names nothing here produces: {paste(consumed_not_produced, collapse = ', ')}"
    )
  }
  invisible(contract)
}

`%||%` <- function(a, b) if (is.null(a)) b else a

if (sys.nframe() == 0L) main()