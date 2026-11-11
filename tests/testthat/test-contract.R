# Output contract checks (roadmap issue #16).
#
# These grep the package source, so they only run from a source checkout
# (devtools::test() or testthat::test_local()). They go away once #24 replaces
# the upload_hmis_data() calls with a validated publish step.

root <- normalizePath(testthat::test_path("..", ".."), mustWork = FALSE)
contract_path <- file.path(root, "inst", "contracts", "outputs.yml")

skip_if_no_source <- function() {
  testthat::skip_if_not(dir.exists(file.path(root, "R")), "package source not available")
  testthat::skip_if_not_installed("yaml")
}

load_contract <- function() {
  testthat::expect_true(
    file.exists(contract_path),
    info = "inst/contracts/outputs.yml is missing. Run dev/build_contract.R."
  )
  yaml::read_yaml(contract_path)
}

upload_calls_in_R <- function() {
  files <- sort(list.files(file.path(root, "R"), pattern = "\\.R$", full.names = TRUE))
  counts <- vapply(files, function(f) {
    lines <- readLines(f, warn = FALSE)
    hits <- grep("upload_hmis_data\\(", lines)
    sum(!grepl("^\\s*#", lines[hits]))
  }, integer(1))
  counts <- counts[counts > 0]
  names(counts) <- paste0("R/", basename(names(counts)))
  counts
}

test_that("every upload_hmis_data() call in R/ maps to a contract entry", {
  skip_if_no_source()
  contract <- load_contract()
  calls <- upload_calls_in_R()

  entries <- contract$files
  src  <- vapply(entries, function(e) e$source, "")
  site <- vapply(entries, function(e) as.integer(e$call_site), 1L)
  dyn  <- vapply(entries, function(e) isTRUE(e$dynamic), TRUE)

  in_R <- startsWith(src, "R/")
  key  <- paste(src, site)

  for (f in names(calls)) {
    for (i in seq_len(calls[[f]])) {
      n <- sum(key == paste(f, i))
      expect_gte(n, 1, label = paste("contract entries for call", i, "in", f))
      # A call site that publishes one literal file maps to exactly one entry.
      # Run-time-named call sites (dynamic) fan out to several.
      if (n >= 1 && !any(dyn[key == paste(f, i)])) {
        expect_equal(n, 1L, label = paste("contract entries for call", i, "in", f))
      }
    }
  }

  # No contract entry points at a call that no longer exists.
  expect_true(all(src[in_R] %in% names(calls)),
              info = "contract references an R/ file with no upload_hmis_data() call")
  for (f in unique(src[in_R])) {
    expect_lte(max(site[src == f]), calls[[f]])
  }
})

test_that("contract file names are unique within a format", {
  skip_if_no_source()
  entries <- load_contract()$files
  id <- vapply(entries, function(e) paste(e$name, e$format, sep = "."), "")
  expect_false(anyDuplicated(id) > 0, info = paste(id[duplicated(id)], collapse = ", "))
})

test_that("outputs.yml holds structure only", {
  skip_if_no_source()
  contract <- load_contract()
  allowed_entry_fields <- c(
    "name", "format", "producer", "source", "call_site", "dynamic", "reviewed",
    "consumers", "in_s3", "schema", "structure", "may_be_empty", "keys",
    "review_columns"
  )
  for (e in contract$files) {
    expect_true(all(names(e) %in% allowed_entry_fields),
                info = paste(e$name, "has unexpected fields:",
                             paste(setdiff(names(e), allowed_entry_fields), collapse = ", ")))
    # Parquet schemas map column name -> arrow type string. A value that is not
    # a type string would be a data value.
    if (!is.null(e$schema)) {
      expect_true(all(vapply(e$schema, is.character, TRUE)), info = e$name)
    }
  }
})