#' @include guidance.R relevant_dq.R aaa_imports.R
NULL

.onLoad <- function(libname, pkgname) {
  # Don't authenticate or load data during package installation
  # This will be handled when functions are actually called
}

#' Ensure Google Sheets authentication is set up
#' @keywords internal
ensure_authenticated <- function() {
  if (!googlesheets4::gs4_has_token()) {
    cred_path <- Sys.getenv("GOOGLE_SERVICE_ACCOUNT_PATH")
    if (cred_path == "") {
      stop("GOOGLE_SERVICE_ACCOUNT_PATH environment variable not set. Use usethis::edit_r_environ() to set it.")
    }
    if (!file.exists(cred_path)) {
      stop("Service account file not found at: ", cred_path)
    }
    googlesheets4::gs4_auth(path = cred_path)
  }
}

#' Load guidance data from Google Sheets
#' @keywords internal
load_guidance_data <- function() {
  ensure_authenticated()

  id <- Sys.getenv("GOOGLE_SHEETS_ID")
  if (id == "") {
    stop("GOOGLE_SHEETS_ID environment variable not set. Use usethis::edit_r_environ() to set it.")
  }

  guidance <- purrr::map(rlang::set_names(googlesheets4::sheet_names(id)),
                         ~googlesheets4::read_sheet(id, sheet = .x, col_types = "c"))

  # Handle irrelevant
  all_dq <- stringr::str_subset(ls(envir = .getNamespace("HMISserve"), pattern = "^dq\\_"),
                                "^((?!\\_sp\\_)(?!\\_overlaps)(?!\\_check_eligibility).)*$")
  irrelevant <- guidance$`Guidance list`$name[(nchar(guidance$`Guidance list`$irrelevant) > 0) %|% FALSE]
  irrelevant <- unique(guidance$Checks$DQ_Check[stringr::str_extract(guidance$Checks$Guidance, "(?<=guidance\\$)[\\w\\_]+") %in% irrelevant])
  relevant_dq <- setdiff(all_dq, irrelevant)

  # Save relevant_dq
  dump("relevant_dq", file.path("R","relevant_dq.R"))

  # Process guidance
  guidance <- purrr::map(rlang::set_names(guidance$`Guidance list`$name), ~{
    guidance$`Guidance list`$guidance[guidance$`Guidance list`$name == .x]
  })

  # Save guidance
  f <- ifelse(clarity.looker::is_dev(), "R/guidance.R",
              file.path(system.file(package = "RmData"), "R", "guidance.R"))
  dump("guidance", f)

  list(guidance = guidance, relevant_dq = relevant_dq)
}

# Create lazy-loaded versions of your exported objects
guidance <- NULL
relevant_dq <- NULL

# You'll need to modify any functions that use these to call load_guidance_data() first
# For example:
#' Get guidance data
#' @export
get_guidance <- function() {
  if (is.null(guidance)) {
    data <- load_guidance_data()
    guidance <<- data$guidance
    relevant_dq <<- data$relevant_dq
  }
  guidance
}

#' Get relevant DQ checks
#' @export
get_relevant_dq <- function() {
  if (is.null(relevant_dq)) {
    data <- load_guidance_data()
    guidance <<- data$guidance
    relevant_dq <<- data$relevant_dq
  }
  relevant_dq
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
