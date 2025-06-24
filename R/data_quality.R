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


data_quality <- function(check_fns = HMISserve::relevant_dq) {
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

  dq_aps_no_referrals <- dqu_aps(Project = Project, data_APs = FALSE, Referrals = Referrals_full)
  dq_APs <- dqu_aps(Project = Project, data_APs = FALSE, Referrals = Referrals_full)

  dq_data <- list()
  dq_data$dq_past_year <- dq_past_year
  dq_data$dq_overlaps <- dq_overlaps
  dq_data$dq_providers <- dq_providers
  dq_data$dq_aps_no_referrals <- dq_aps_no_referrals
  dq_data$dq_APs <- dq_APs
  dq_data$dq_eligibility_detail <- dq_eligibility_detail
  dq_data$dq_for_pe <- dq_for_pe

  return(dq_data)
}
