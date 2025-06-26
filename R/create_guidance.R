# In create_guidance.R

# Package-level cache
.guidance_cache <- new.env(parent = emptyenv())

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
#' @param force Logical. If TRUE, forces a refresh from Google Sheets even if cached data exists
#' @keywords internal
load_guidance_data <- function(force = FALSE) {
  # Check cache first unless forced
  if (!force && exists("guidance", envir = .guidance_cache) && exists("relevant_dq", envir = .guidance_cache)) {
    return(list(
      guidance = get("guidance", envir = .guidance_cache),
      relevant_dq = get("relevant_dq", envir = .guidance_cache)
    ))
  }

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
  relevant_dq_vector <- setdiff(all_dq, irrelevant)

  # Process guidance
  processed_guidance <- purrr::map(rlang::set_names(guidance$`Guidance list`$name), ~{
    guidance$`Guidance list`$guidance[guidance$`Guidance list`$name == .x]
  })

  # Cache the results
  assign("guidance", processed_guidance, envir = .guidance_cache)
  assign("relevant_dq", relevant_dq_vector, envir = .guidance_cache)
  assign("last_updated", Sys.time(), envir = .guidance_cache)

  list(guidance = processed_guidance, relevant_dq = relevant_dq_vector)
}

#' Get guidance data
#'
#' Returns a list of instructions tailored to the specific HMIS software
#' for each Data Quality Issue. Data is cached and only refreshed when
#' explicitly requested.
#'
#' @param refresh Logical. If TRUE, forces a refresh from Google Sheets
#' @return A named list of guidance instructions
#' @export
get_guidance <- function(refresh = FALSE) {
  data <- load_guidance_data(force = refresh)
  data$guidance
}

#' Get relevant DQ check function names
#'
#' Returns a character vector containing the names of all relevant data quality
#' check functions based on current guidance configuration.
#'
#' @param refresh Logical. If TRUE, forces a refresh from Google Sheets
#' @return A character vector with data quality check function names
#' @export
relevant_dq <- function(refresh = FALSE) {
  data <- load_guidance_data(force = refresh)
  data$relevant_dq
}

#' Refresh guidance data from Google Sheets
#'
#' Forces a refresh of both guidance and relevant DQ data from Google Sheets.
#' This updates the in-memory cache without requiring package reinstallation.
#'
#' @return Invisibly returns the timestamp of the update
#' @export
refresh_guidance <- function() {
  load_guidance_data(force = TRUE)
  cat("Guidance data refreshed at", format(Sys.time()), "\n")
  invisible(get("last_updated", envir = .guidance_cache))
}

#' Get guidance cache info
#'
#' Returns information about the current guidance cache status
#'
#' @return A list with cache status information
#' @export
guidance_cache_info <- function() {
  if (exists("last_updated", envir = .guidance_cache)) {
    list(
      cached = TRUE,
      last_updated = get("last_updated", envir = .guidance_cache),
      guidance_items = length(get("guidance", envir = .guidance_cache)),
      relevant_dq_count = length(get("relevant_dq", envir = .guidance_cache))
    )
  } else {
    list(cached = FALSE)
  }
}
