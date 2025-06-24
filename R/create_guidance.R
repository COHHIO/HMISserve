#' @include guidance.R relevant_dq.R aaa_imports.R
NULL

.onLoad <- function(libname, pkgname) {
  googlesheets4::gs4_auth(path = "inst/vault/rminor@rminor-333915.iam.gserviceaccount.com.json")
  id <- "15HsbSGmsscGtUIZnBDSVPaU4Zsotp7Dj79mXpPAu_lw"
  guidance <- purrr::map(rlang::set_names(googlesheets4::sheet_names(id)), ~googlesheets4::read_sheet(id, sheet = .x, col_types = "c"))
  # Handle irrelevant
  all_dq <- stringr::str_subset(ls(envir = .getNamespace("HMISserve"), pattern = "^dq\\_"), "^((?!\\_sp\\_)(?!\\_overlaps)(?!\\_check_eligibility).)*$")

  irrelevant <- guidance$`Guidance list`$name[(nchar(guidance$`Guidance list`$irrelevant) > 0) %|% FALSE]
  irrelevant <- unique(guidance$Checks$DQ_Check[stringr::str_extract(guidance$Checks$Guidance, "(?<=guidance\\$)[\\w\\_]+") %in% irrelevant])
  relevant_dq <- setdiff(all_dq, irrelevant)


  dump("relevant_dq", file.path("R","relevant_dq.R"))
  guidance <- purrr::map(rlang::set_names(guidance$`Guidance list`$name), ~{
    guidance$`Guidance list`$guidance[guidance$`Guidance list`$name == .x]
  })
  f <- ifelse(clarity.looker::is_dev(), "R/guidance.R", file.path(system.file(package = "RmData"), "R", "guidance.R"))

  dump("guidance", f)
}

#' @title guidance
#' @name guidance
#' @description A list of instructions tailored to the specific HMIS software for each Data Quality Issue.
#' @export
guidance

#' @title Relevant dq
#' @name relevant_dq
#' @description A list of the Data Quality Checks that are relevant and should be run in `data_quality`.
#' @export
relevant_dq


