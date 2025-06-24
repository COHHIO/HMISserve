qpr_project_small <- function(Project, rm_dates) {
  Project |>
    dplyr::select(ProjectID,
                  OrganizationName,
                  OperatingStartDate,
                  OperatingEndDate,
                  ProgramCoC,
                  ProjectName,
                  ProjectType,
                  HMISParticipationType,
                  GrantType,
                  ProjectCounty,
                  ProjectRegion) |>
    HMIS::operating_between(rm_dates$calc$data_goes_back_to, rm_dates$meta_HUDCSV$Export_End) |>
    dplyr::filter(HMISParticipationType == 1 &
                    !is.na(ProjectRegion) &
                    ProjectType %in% c(0:4, 6, 8:9, 12:14))
}

qpr_enrollment_small <- function(Enrollment_extra_Client_Exit_HH_CL_AaE) {
  Enrollment_extra_Client_Exit_HH_CL_AaE |>
    dplyr::select(
      dplyr::all_of(c(
        "CountyServed",
        "DateCreated",
        "Destination",
        "EnrollmentID",
        "EntryAdjust",
        "EntryDate",
        "ExitAdjust",
        "ExitDate",
        "HouseholdID",
        "LivingSituation",
        "MoveInDate",
        "MoveInDateAdjust",
        "PersonalID",
        "ProjectID",
        "RelationshipToHoH",
        "UniqueID"
      ))
    )
}

qpr_validation <- function(project_small, enrollment_small) {
  project_small |>
    dplyr::left_join(enrollment_small, by = "ProjectID") |>
    dplyr::select(
      ProjectID,
      ProjectName,
      ProjectType,
      CountyServed,
      EnrollmentID,
      PersonalID,
      HouseholdID,
      RelationshipToHoH,
      EntryDate,
      EntryAdjust,
      MoveInDate,
      MoveInDateAdjust,
      ExitDate,
      LivingSituation,
      Destination,
      DateCreated,
      UniqueID
    ) |>
    dplyr::filter(!is.na(EntryDate))
}

qpr_mental_health <- function(validation, Disabilities, app_env = get_app_env(e = rlang::caller_env())) {
  dplyr::left_join(validation, dplyr::select(Disabilities, dplyr::ends_with("ID"), DisabilityType, DateUpdated), by = c("PersonalID", "EnrollmentID")) |>
    dplyr::filter(DisabilityType %in% c(9, # Mental Health
                                        10 # Substance Abuse
    ) &
      CountyServed %in% c("Lake", "Lorain", "Trumbull") &
      is.na(ExitDate) & # Assumed actively enrolled
      LivingSituation %in% HMISprep::data_types$CurrentLivingSituation$CurrentLivingSituation$homeless) |>
    dplyr::distinct(UniqueID, PersonalID, LivingSituation, DisabilityType) |>
    dplyr::group_by(DisabilityType) |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::mutate(DisabilityType = HMIS::hud_translations$`1.3 DisabilityType`(DisabilityType))

}

qpr_path_to_rrhpsh <- function(Enrollment_extra_Client_Exit_HH_CL_AaE, Referrals) {
  rrh_psh_expr <- stringr::str_subset(c(names(Enrollment_extra_Client_Exit_HH_CL_AaE), names(Referrals)), UU::regex_or(c("ProjectType", "ReferringPTC$"))) |>
    paste("%in% c(3, 13, 'PH \u2013 Rapid Re-Housing', 'PH \u2013 Permanent Supportive Housing (disability required for entry)')") |> #RRH or PSH Respectively
    purrr::map(rlang::parse_expr)

  in_path <- Enrollment_extra_Client_Exit_HH_CL_AaE |>
    HMIS::served_between(start = Sys.Date() - lubridate::years(1), end = lubridate::today()) |>
    dplyr::filter(ClientEnrolledInPATH == 1 & (!is.na(ExitDate) & EntryDate > (Sys.Date() - lubridate::years(1))))
  total_path <- in_path |>
    dplyr::pull(PersonalID) |>
    unique() |>
    length()
  path_referrals <- in_path |>
    dplyr::filter(!!purrr::reduce(rrh_psh_expr, ~rlang::expr(!!.x | !!.y))) |>
    dplyr::select(dplyr::any_of(paste0(c("Enrollment", "Personal", "Unique", "Household"), "ID")), EntryDate, ExitDate, ProjectID, ProjectType, dplyr::starts_with("R_")) |>
    dplyr::arrange(PersonalID, dplyr::desc(EntryDate)) |>
    # needs to count individuals
    # needs to be individuals in, referred by, referred to
    dplyr::group_by(!!!rlang::syms(stringr::str_subset(c(names(Enrollment_extra_Client_Exit_HH_CL_AaE), names(Referrals)), UU::regex_or(c("ProjectType", "PTC$"))) |> unique())) |>
    dplyr::filter(is.na(R_ReferringPTC) | R_ReferringPTC != 2) |>
    dplyr::distinct(PersonalID, .keep_all = TRUE) |>
    dplyr::mutate(rrhpsh = dplyr::case_when(
      ProjectType %in% c(13) ~ "RRH",
      ProjectType %in% c(3) ~ "PSH",
      R_ReferringPTC %in% c(3) ~ "PSH",
      R_ReferringPTC %in% c(13) ~ "PSH",
    )) |>
    dplyr::group_by(rrhpsh) |>
    dplyr::summarise(n = dplyr::n(),
                     `PctPATH` = scales::percent(n / total_path, accuracy = 0.01))

}
