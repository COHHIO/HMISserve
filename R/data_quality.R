#' @include app_dependencies.R 04_DataQuality_utils.R
dependencies <- list()
dependencies$DataQuality <-
  c(
    "Client",
    "Contacts",
    "Disabilities",
    "Enrollment_extra_Client_Exit_HH_CL_AaE",
    "Funder",
    "guidance",
    "HealthAndDV",
    "IncomeBenefits",
    "Inventory",
    "living_situation",
    "mahoning_projects",
    "rm_dates",
    "Project",
    "Referrals",
    "Scores",
    "Services_enroll_extras",
    "Users"
  )


#' Title
#'
#' @param check_fns
#'
#' @return
#' @export
#'
#' @examples
data_quality <- function(.deps) {
  required <- c(
    "Client", "Project", "Contacts", "Disabilities",
    "Enrollment_extra_Client_Exit_HH_CL_AaE", "Funder",
    "guidance", "HealthAndDV", "IncomeBenefits", "Inventory",
    "living_situation", "mahoning_projects", "rm_dates",
    "Referrals", "Referrals_full", "Scores", "Services_enroll_extras", "Users",
    "vars", "living_situation", "guidance", "check_fns"
  )

  # Check for missing deps
  missing <- setdiff(required, names(.deps))
  if (length(missing) > 0) {
    stop("Missing dependencies: ", paste(missing, collapse = ", "))
  }

  # Extract objects from .deps into local environment
  list2env(.deps, envir = environment())

  # Providers to Check ------------------------------------------------------
  projects_current_hmis <- projects_current_hmis(Project = Project,
                                                 Inventory = Inventory,
                                                 rm_dates = rm_dates)

  vars <- make_vars()

  # Clients to Check --------------------------------------------------------

  served_in_date_range <- served_in_date_range(projects_current_hmis = projects_current_hmis,
                                               Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
                                               Client = Client,
                                               Project = Project,
                                               Inventory = Inventory,
                                               HealthAndDV = HealthAndDV,
                                               vars = vars,
                                               rm_dates = rm_dates)


  ssvf_served_in_date_range <- ssvf_served_in_date_range(Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
                                                         served_in_date_range = served_in_date_range,
                                                         Client = Client)
  # app_env$gather_deps(ssvf_served_in_date_range)

  .total <- length(check_fns)

  .pid <- cli::cli_progress_bar(type = "iterator",
                                total = .total + 4)

  dqs <- purrr::map(rlang::set_names(check_fns), ~{
    i <- which(check_fns == .x)
    cli::cli_progress_update(id = .pid,,
                             status = paste0(i,"/",.total,": ",stringr::str_remove(.x, "^dq\\_")))
    fn <- getFromNamespace(.x, "HMISserve")
    arg_names <- rlang::set_names(rlang::fn_fmls_names(fn))
    arg_names <- arg_names[!purrr::map_lgl(rlang::fn_fmls(fn), is.logical)]
    arg_names <- arg_names[arg_names != c("app_env")]

    .call <- rlang::call2(fn, !!!purrr::map(arg_names, ~rlang::sym(.x)))

    out <- rlang::eval_bare(.call)|>
      dplyr::distinct(PersonalID, EnrollmentID, Issue, .keep_all = TRUE) |>
      dplyr::mutate_all(as.character)
    UU::join_check(out)
    out
  })

  cli::cli_progress_update(id = .pid,,
                           status = "Creating data quality table")

  dq_main <- do.call(rbind, dqs) |>
    unique() |>
    dplyr::mutate(Type = factor(Type, levels = c("High Priority",
                                                 "Error",
                                                 "Warning"))) |>
    dplyr::filter(ProjectType != 14 |
                    (
                      ProjectType == 14 &
                        Issue %in% c(
                          "60 Days in Mahoning Coordinated Entry",
                          "Access Point with Entry Exits",
                          "No Head of Household",
                          "Missing Date of Birth Data Quality",
                          "Don't Know/Prefers Not to Answer Approx. Date of Birth",
                          "Missing DOB",
                          "Missing Name Data Quality",
                          "Incomplete or Don't Know/Prefers Not to Answer Name",
                          "Rent Payment Made, No Move-In Date",
                          "Invalid SSN",
                          "Don't Know/Prefers Not to Answer SSN",
                          "Missing Gender",
                          "Missing SSN",
                          "Missing Race and Ethnicity",
                          "Missing Relationship to Head of Household",
                          "Missing Veteran Status",
                          "Don't Know/Prefers Not to Answer Veteran Status",
                          "Missing County Served",
                          "Duplicate Entry Exits"
                        )
                    )) |>
    dplyr::filter(Issue != "Old Outstanding Referral")
  cli::cli_progress_update(id = .pid,,
                           status = "Finish dq_main")


  dq_past_year <- HMIS::served_between(dq_main, rm_dates$hc$check_dq_back_to, lubridate::today())
  dq_for_pe <- HMIS::served_between(dq_main, rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end)

  cli::cli_progress_update(id = .pid,
                           status = "Overlapping Project Stays")

  dq_overlaps <- dq_overlaps(served_in_date_range = served_in_date_range,
                             vars = vars,
                             guidance = guidance,
                             data_types = HMISprep::data_types)

  cli::cli_progress_update(id = .pid,
                           status = "Eligibility Checks")

  dq_eligibility_detail <- dq_check_eligibility(served_in_date_range = served_in_date_range,
                                                mahoning_projects = mahoning_projects,
                                                vars = vars,
                                                rm_dates = rm_dates)

  dq_providers <- rlang::set_names(projects_current_hmis$ProjectID, projects_current_hmis$ProjectName)[order(projects_current_hmis$ProjectName)]

  dq_providers_df <- data.frame(
    ProjectID = as.character(dq_providers),
    ProjectName = names(dq_providers),
    stringsAsFactors = FALSE
  )

  dq_aps_no_referrals <- dqu_aps(Project = Project, data_APs = FALSE, Referrals = Referrals_full)

  dq_APs <- dqu_aps(Project = Project, data_APs = TRUE, Referrals = Referrals_full)

  dq_data_files <- list(
    "dq_past_year" = dq_past_year,
    "dq_overlaps" = dq_overlaps,
    "dq_providers_df" = dq_providers_df,
    "dq_aps_no_referrals" = dq_aps_no_referrals,
    "dq_APs" = dq_APs,
    "dq_eligibility_detail" = dq_eligibility_detail,
    "dq_for_pe" = dq_for_pe,
    "dq_main" = dq_main
  )

  # Upload all files
  upload_results <- purrr::imap_lgl(dq_data_files, ~ {
    HMISdata::upload_hmis_data(
      data = .x,
      file_name = .y,
      bucket = "shiny-data-cohhio",
      folder = "RME",
      format = "parquet"
    )
  })

  # Check results
  if (all(upload_results)) {
    cli::cli_alert_success("All DQ data files uploaded successfully")
  } else {
    failed_files <- names(upload_results)[!upload_results]
    cli::cli_alert_warning("Failed to upload: {paste(failed_files, collapse = ', ')}")
  }

  return(dq_data_files)
}
