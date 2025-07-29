# this script uses the HMIS data to compare the avg SPDAT score of
# clients served in a county during the reporting period to the avg
# SPDAt score of those who enrolled into a PSH or RRH project during the
# reporting period.
qpr_spdats <- function(Enrollment_extra_Client_Exit_HH_CL_AaE,
                       Project,
                       Regions,
                       Scores,
                       data_types

) {
  # more paring down, only taking what variables I need from Enrollment
  enrollment_small <- Enrollment_extra_Client_Exit_HH_CL_AaE %>%
    dplyr::left_join(Project, by = c("ProjectType", "ProjectID", "ProjectName")) %>%
    dplyr::select(
      EnrollmentID,
      UniqueID,
      PersonalID,
      ProjectID,
      ProjectType,
      ProjectName,
      OperatingStartDate,
      OperatingEndDate,
      EntryDate,
      ExitDate,
      RelationshipToHoH,
      CountyServed
    ) |>
    dplyr::left_join(Regions, by = c("CountyServed" = "County"))
  # Entries will give us all the times a hh has an Entry into a PH project
  Entries <- enrollment_small %>%
    dplyr::filter(ProjectType %in% data_types$Project$ProjectType$ph)


  # this object is used in the app to create the plot. it has date variables
  # included so the numbers can be filtered by date range in the app. it takes
  # long to run.

  qpr_spdats_county <-
    dplyr::left_join(enrollment_small, Scores |> dplyr::mutate(PersonalID = as.character(PersonalID)),
                     by = UU::common_names(enrollment_small, Scores)) %>%
    dplyr::filter(
      ProjectType %in% c(0, 1, 2, 4, 8) &
        RelationshipToHoH == 1 &
        ScoreDate <= EntryDate &
        !is.na(Region)
    ) %>%
    dplyr::select(
      EnrollmentID,
      UniqueID,
      PersonalID,
      ProjectName,
      EntryDate,
      ExitDate,
      CountyServed,
      Region,
      ScoreDate,
      Score
    ) %>%
    dplyr::group_by(PersonalID) %>%
    dplyr::slice_max(EntryDate) %>% # most recent EE
    dplyr::slice_max(ScoreDate) %>% # most recent score
    dplyr::slice_max(Score) %>% # highest score
    dplyr::ungroup() %>%
    dplyr::select(PersonalID,
                  UniqueID,
                  Region,
                  ProjectName,
                  CountyServed,
                  Score,
                  EntryDate,
                  ExitDate)

  # this pulls all entries into PSH or RRH
  entry_scores <- dplyr::left_join(Entries, Scores |> dplyr::mutate(PersonalID = as.character(PersonalID)),
                                   by = UU::common_names(Entries, Scores))

  qpr_spdats_project <- entry_scores %>%
    dplyr::select(-ProjectType,
                  -OperatingStartDate,
                  -OperatingEndDate) %>%
    dplyr::mutate(ScoreDate = as.Date(ScoreDate)) |>
    dplyr::filter(
      RelationshipToHoH == 1 &
        (ScoreDate <= EntryDate | is.na(ScoreDate)) &
        !is.na(Region)
    ) %>%
    dplyr::mutate(
      ScoreAdjusted = dplyr::if_else(is.na(Score), 0, Score),
      ScoreDateAdjusted = dplyr::if_else(is.na(ScoreDate), lubridate::today(), ScoreDate)
    ) %>%
    dplyr::group_by(EnrollmentID) %>%
    dplyr::slice_max(ScoreDateAdjusted) %>%
    dplyr::slice_max(ScoreAdjusted) %>%
    dplyr::distinct() %>%
    dplyr::ungroup() %>%
    dplyr::select(-ScoreDateAdjusted, -RegionName)

  # If you have clients here, you should either verify the scores saved here are
  # valid or the correct client is marked as the Head of Household.

  # SPDATsOnNonHoHs <- entry_scores %>%
  #   HMIS::served_between(rm_dates$calc$data_goes_back_to, rm_dates$meta_HUDCSV$Export_End) |>
  #   dplyr::filter(RelationshipToHoH != 1 &
  #            !is.na(Score)) %>%
  #   dplyr::select(ProjectName, PersonalID, UniqueID, EntryDate, ExitDate, Score) %>%
  #   dplyr::arrange(ProjectName)


  HMISdata::upload_hmis_data(qpr_spdats_county,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "qpr_spdats_county.parquet", format = "parquet")
  HMISdata::upload_hmis_data(qpr_spdats_project,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "qpr_spdats_project.parquet", format = "parquet")

}
