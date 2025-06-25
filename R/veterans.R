#' Title
#'
#' @param Client
#' @param Enrollment_extra_Client_Exit_HH_CL_AaE
#' @param Project
#' @param VeteranCE
#' @param tay
#'
#' @return
#' @export
#'
#' @examples
vets <- function(Client,
                 Enrollment_extra_Client_Exit_HH_CL_AaE,
                 Project,
                 VeteranCE,
                 tay) {
  # getting all the veterans
  Veterans <- Client |>
    dplyr::filter(VeteranStatus == 1) |>
    dplyr::select(PersonalID,
                  dplyr::all_of(c(HMISprep::col_cats$Client$gender,
                                  HMISprep::col_cats$Client$race)))

  # getting all the EE data of all the veterans
  VeteranHHs <- Veterans |>
    dplyr::left_join(Enrollment_extra_Client_Exit_HH_CL_AaE, by = c("PersonalID")) |>
    dplyr::select(
      PersonalID,
      UniqueID,
      ProjectID,
      EnrollmentID,
      EntryDate,
      HouseholdID,
      RelationshipToHoH,
      LivingSituation,
      LengthOfStay,
      LOSUnderThreshold,
      PreviousStreetESSH,
      DateToStreetESSH,
      TimesHomelessPastThreeYears,
      MonthsHomelessPastThreeYears,
      DisablingCondition,
      DateOfEngagement,
      MoveInDate,
      VAMCStation,
      CountyServed,
      CountyPrior,
      ExitDate,
      Destination,
      OtherDestination,
      ExitAdjust,
      AgeAtEntry
    )

  # adding in all the provider data
  VeteranHHs <- Project |>
    HMISprep::Project_rm_zz() |>
    dplyr::select(
      ProjectID,
      OrganizationName,
      OperatingStartDate,
      OperatingEndDate,
      ProjectType,
      GrantType,
      ProjectName,
      ProjectRegion
    ) |>
    dplyr::right_join(VeteranHHs, by = "ProjectID")

  VeteranHHs <- VeteranHHs |>
    dplyr::left_join(VeteranCE |>
                       dplyr::mutate_at(dplyr::vars("PersonalID", "EnrollmentID", "UniqueID"), as.character),
                     by = c("PersonalID", "EnrollmentID", "UniqueID"))

  CurrentVeterans <- VeteranHHs |>
    dplyr::filter((ProjectType %in% c(0, 1, 2, 4, 8, 12) & (
      EntryDate <= Sys.Date() &
        (is.na(ExitDate) | ExitDate > Sys.Date())
    )) |
      (ProjectType %in% c(3, 9, 13) & (
        MoveInDate <= Sys.Date() &
          (is.na(ExitDate) |
             ExitDate > Sys.Date())
      ))) |>
    dplyr::mutate(ProjectRegion = dplyr::if_else(is.na(ProjectRegion),
                                                 -1, # must be numeric - Projects with NA regions are those outside of the Balance of State
                                                 ProjectRegion))

  CurrentVeteranCounts <- CurrentVeterans |>
    dplyr::filter(ProjectType %in% c(0, 1, 2, 4, 8)) |>
    dplyr::group_by(ProjectName, ProjectRegion) |>
    dplyr::summarise(Veterans = dplyr::n(), .groups = "drop")

  VeteranEngagement <- CurrentVeterans |>
    dplyr::filter(ProjectType %in% c(0, 1, 2, 4, 8)) |>
    dplyr::mutate(
      EngagementStatus = dplyr::case_when(
        !is.na(PHTrack) & PHTrack != "None" &
          ExpectedPHDate >= Sys.Date() ~ "Has Current Housing Plan",
        is.na(PHTrack) | PHTrack == "None" |
          (!is.na(PHTrack) & (
            ExpectedPHDate < Sys.Date() |
              is.na(ExpectedPHDate)
          )) ~ "No Current Housing Plan"
      )
    ) |>
    dplyr::select(
      ProjectName,
      ProjectType,
      ProjectRegion,
      PersonalID,
      PHTrack,
      ExpectedPHDate,
      EngagementStatus
    )

  vets_current <- VeteranEngagement |>
    dplyr::group_by(ProjectName, ProjectType, ProjectRegion, EngagementStatus) |>
    dplyr::summarise(CurrentVeteranCount = dplyr::n(), .groups = "drop") |>
    tidyr::pivot_wider(names_from = EngagementStatus, values_from = CurrentVeteranCount) |>
    dplyr::rename(dplyr::any_of(c(HasCurrentHousingPlan = "Has Current Housing Plan",
                                  NoCurrentHousingPlan = "No Current Housing Plan")))

  cols <- c(HasCurrentHousingPlan = NA_real_, NoCurrentHousingPlan = NA_real_)

  vets_current <- tibble::add_column(vets_current,
                                     !!!cols[setdiff(names(cols), names(vets_current))])

  vets_current[is.na(vets_current)] <- 0

  vets_current <- vets_current |>
    dplyr::mutate(
      Summary =
        dplyr::case_when(
          HasCurrentHousingPlan == 0 &
            NoCurrentHousingPlan == 1 ~
            "This veteran has no current Housing Plan",
          HasCurrentHousingPlan == 0 &
            NoCurrentHousingPlan > 1  ~
            "None of these veterans have current Housing Plans",
          HasCurrentHousingPlan == 1 &
            NoCurrentHousingPlan == 0 ~
            "This veteran has a current Housing Plan!",
          HasCurrentHousingPlan > 1 &
            NoCurrentHousingPlan == 0  ~
            "All veterans in this project have current Housing Plans!",
          HasCurrentHousingPlan == 1 &
            NoCurrentHousingPlan > 0 ~
            paste(HasCurrentHousingPlan,
                  "of these veterans has a current Housing Plan"),
          HasCurrentHousingPlan > 1 &
            NoCurrentHousingPlan > 0 ~
            paste(HasCurrentHousingPlan,
                  "of these veterans have current Housing Plans")
        )
    ) |>
    dplyr::left_join(CurrentVeteranCounts, by = c("ProjectName", "ProjectRegion")) |>
    dplyr::ungroup()

  current_tay_hohs <- tay |>
    dplyr::filter(RelationshipToHoH == 1 &
                    is.na(ExitDate) &
                    ProjectType %in% c(0, 1, 2, 4, 8))  |>
    {\(x) {
      dplyr::group_by(x, ProjectName, ProjectType) |>
        dplyr::summarise(TAYHHs = sum(TAY, na.rm = TRUE), .groups = "drop") |>
        dplyr::left_join(x, by = c("ProjectName", "ProjectType"))
    }}() |>
    dplyr::select(PersonalID,
                  EnrollmentID,
                  ProjectName,
                  ProjectType,
                  TAYHHs) |>
    dplyr::left_join(
      VeteranCE |>
        dplyr::mutate_at(dplyr::vars(PersonalID, EnrollmentID), as.character) |>
        dplyr::mutate(ExpectedPHDate = as.Date(ExpectedPHDate)) |>
        dplyr::select(PersonalID,
                      EnrollmentID,
                      PHTrack,
                      ExpectedPHDate),
      by = c("PersonalID", "EnrollmentID")
    ) |>
    dplyr::mutate(HasPlan = dplyr::if_else(
      !is.na(PHTrack) & PHTrack != "None" &
        !is.na(ExpectedPHDate) &
        ExpectedPHDate >= Sys.Date(),
      1,
      0
    )) |>
    dplyr::group_by(ProjectName, ProjectType, TAYHHs, HasPlan) |>
    dplyr::summarise(HasPlan = sum(HasPlan, na.rm = TRUE), .groups = "drop")  |>
    dplyr::mutate(
      Summary =
        dplyr::case_when(
          HasPlan == 0 &
            TAYHHs == 1 ~
            "This Transition Aged Youth household has no current Housing Plan",
          HasPlan == 0 &
            TAYHHs > 1  ~
            "None of these Transition Aged Youth households have current Housing Plans",
          HasPlan == 1 &
            TAYHHs == 1 ~
            "This Transition Aged Youth household has a current Housing Plan!",
          HasPlan == TAYHHs &
            HasPlan > 0 ~
            "All Transition Aged Youth households in this project have current Housing Plans!",
          HasPlan == 1 &
            HasPlan != TAYHHs ~
            paste(
              HasPlan,
              "of these Transition Aged Youth households has a current Housing Plan"
            ),
          HasPlan > 1 &
            HasPlan != TAYHHs ~
            paste(
              HasPlan,
              "of these Transition Aged Youth households have current Housing Plans"
            )
        )
    )

  HMISdata::upload_hmis_data(vets_current, file_name = "vets_current.parquet", format = "parquet")
  HMISdata::upload_hmis_data(current_tay_hohs, file_name = "current_tay_hohs.parquet", format = "parquet")

}
