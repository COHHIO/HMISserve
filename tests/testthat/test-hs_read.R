# hs_read() routing (roadmap issue #17). No S3 access and no real data: the
# HMISdata loaders are mocked and only the arguments they receive are checked.

test_that("live sources match what the call sites used before hs_read()", {
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = NA, DATA_ENV = NA)

  expect_equal(hs_source("hmis_parquet"), list(bucket = "hud.csv-daily", folder = "hmis_output"))
  expect_equal(hs_source("hmis_csv"), list(bucket = "hud.csv-daily", folder = "hmis_files"))
  expect_equal(hs_source("looker"), list(bucket = "looker-daily", folder = NULL))
  expect_equal(hs_source("output"), list(bucket = "shiny-data-cohhio", folder = "RME"))
})

test_that("output kind follows DATA_ENV when no prefix is set", {
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = NA, DATA_ENV = "RME_test")
  expect_equal(hs_source("output")$folder, "RME_test")
})

test_that("an empty prefix counts as unset", {
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = "")
  expect_equal(hs_source("hmis_parquet")$bucket, "hud.csv-daily")
})

test_that("a prefix sends every kind to its snapshot subfolder", {
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = "_snapshots/2026-11-11", DATA_ENV = "RME_test")

  expect_equal(hs_source("hmis_parquet"),
               list(bucket = "shiny-data-cohhio", folder = "_snapshots/2026-11-11/hmis_output"))
  expect_equal(hs_source("hmis_csv")$folder, "_snapshots/2026-11-11/hmis_files")
  expect_equal(hs_source("looker")$folder, "_snapshots/2026-11-11/looker/")
  # DATA_ENV picks the output folder, not the input; snapshots always hold RME/.
  expect_equal(hs_source("output")$folder, "_snapshots/2026-11-11/RME")
})

test_that("leading and trailing slashes in the prefix are ignored", {
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = "/_snapshots/2026-11-11/")
  expect_equal(hs_source("hmis_parquet")$folder, "_snapshots/2026-11-11/hmis_output")
})

test_that("unknown kinds are rejected", {
  expect_error(hs_source("guidance"))
  expect_error(hs_read("x", kind = "nope"))
})

test_that("hs_read() passes the resolved source to the right loader", {
  testthat::skip_if_not_installed("HMISdata")
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = "_snapshots/2026-11-11")

  calls <- list()
  record <- function(fn) function(...) {
    calls[[length(calls) + 1]] <<- c(fn = fn, list(...))
    NULL
  }
  testthat::local_mocked_bindings(
    load_hmis_parquet = record("parquet"),
    load_hmis_csv = record("csv"),
    load_looker_data = record("looker"),
    .package = "HMISdata"
  )

  hs_read("Client.parquet")
  hs_read("Organization.csv", kind = "hmis_csv", col_types = "c")
  hs_read("Client_Offer", kind = "looker", col_types = "c")
  hs_read("dq_for_pe.parquet", kind = "output")

  expect_equal(calls[[1]], list(fn = "parquet", "Client.parquet",
                                bucket = "shiny-data-cohhio",
                                folder = "_snapshots/2026-11-11/hmis_output"))
  expect_equal(calls[[2]], list(fn = "csv", "Organization.csv",
                                bucket = "shiny-data-cohhio",
                                folder = "_snapshots/2026-11-11/hmis_files",
                                col_types = "c"))
  expect_equal(calls[[3]], list(fn = "looker",
                                bucket = "shiny-data-cohhio",
                                prefix = "_snapshots/2026-11-11/looker/",
                                filename = "Client_Offer", col_types = "c"))
  expect_equal(calls[[4]]$folder, "_snapshots/2026-11-11/RME")
})

test_that("a failed read still returns NULL", {
  testthat::skip_if_not_installed("HMISdata")
  testthat::local_mocked_bindings(load_hmis_parquet = function(...) NULL, .package = "HMISdata")
  expect_null(hs_read("Missing.parquet"))
})

test_that("HMISSERVE_LOG_READS=1 logs the key", {
  withr::local_envvar(HMISSERVE_INPUT_PREFIX = "_snapshots/x", HMISSERVE_LOG_READS = "1")
  uri <- hs_log_read("hmis_parquet", hs_source("hmis_parquet"), "Client.parquet")
  expect_equal(uri, "s3://shiny-data-cohhio/_snapshots/x/hmis_output/Client.parquet")
})

test_that("logging is off by default", {
  withr::local_envvar(HMISSERVE_LOG_READS = NA)
  expect_null(hs_log_read("hmis_parquet", hs_source("hmis_parquet"), "Client.parquet"))
})
