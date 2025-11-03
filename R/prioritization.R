#' @title Create the prioritization list
#'
#' @param co_clients_served \code{(data.frame)} See `cohorts`
#' @param Enrollment_extra_Client_Exit_HH_CL_AaE \code{(data.frame)} See `load_export`
#' @param Referrals \code{(data.frame)} See `load_export`
#' @param Scores \code{(data.frame)} See `load_export`
#' @seealso load_export, cohorts
#' @inheritParams R6Classes
#' @inheritParams data_quality_tables
#' @export
#'
#' @include 03_prioritization_utils.R
prioritization <- function(
    co_clients_served,
    Disabilities,
    Enrollment_extra_Client_Exit_HH_CL_AaE,
    HealthAndDV,
    IncomeBenefits,
    Project,
    Referrals,
    Scores,
    data_types
) {
  co_currently_homeless <- co_clients_served |>
    dplyr::filter(is.na(ExitDate) |
                    ExitDate > lubridate::today())

  # get household size here, further down undercounts
  household_size <- co_clients_served |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(HouseholdSize = length(unique(PersonalID))) |>
    dplyr::ungroup() |>
    dplyr::select(PersonalID, HouseholdSize, EnrollmentID)

  # getting the current living situation and
  # comparing to (recent) LivingSituation to get the most recent
  # new variable RecentLivingSituation
  CurrentLivingSituation = HMISdata::load_hmis_csv("CurrentLivingSituation.csv",
                                                   col_types = HMISdata::hmis_csv_specs$CurrentLivingSituation)

  recent_living_situations <- CurrentLivingSituation |>
    dplyr::group_by(EnrollmentID) |>
    dplyr::slice_max(DateUpdated, n = 1, with_ties = FALSE) |>  # or use your recent_valid function
    dplyr::ungroup() |>
    dplyr::select(EnrollmentID, CurrentLivingSituation, DateUpdatedCLS = DateUpdated)

  # filtering for SSO, CE, and SO AND if client is in a homeless situation
  # then filtering for most recent living situation and

  PID_homeless <- Enrollment_extra_Client_Exit_HH_CL_AaE |>
    dplyr::filter(ProjectType %in% c(unlist(data_types$Project$ProjectType[c("so", "ce")]), "street outreach" = 4)) |>
    dplyr::left_join(recent_living_situations, by = "EnrollmentID") |>
    # if the most recent current living situation is most recently updated, get that, not get the living situation from enrollment
    # if NA then get CLS, then get LivingSituation
    dplyr::mutate(RecentLivingSituation = dplyr::coalesce(
      dplyr::if_else(DateUpdatedCLS > DateUpdated, CurrentLivingSituation, LivingSituation),
      CurrentLivingSituation,
      LivingSituation
    )) |>
    dplyr::mutate(DateUpdatedRLS = dplyr::coalesce(
      dplyr::if_else(DateUpdatedCLS > DateUpdated, DateUpdatedCLS, DateUpdated),
      DateUpdatedCLS,
      DateUpdated
    )) |>
    dplyr::group_by(PersonalID) |>
    dplyr::slice_max(DateUpdatedRLS, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::filter(RecentLivingSituation %in% data_types$CurrentLivingSituation$CurrentLivingSituation$homeless) |>
    dplyr::pull(PersonalID)

  # create a ranking of most secure to least secure project type
  importance_ranking <- data.frame(ProjectType = c(3, 10, 9, 13, 7, 2, 0, 1, 8, 11, 14, 4, 6))

  # clients currently entered into a homeless project in our system
  co_currently_homeless <- co_currently_homeless |>
    dplyr::filter(
      ProjectType %in% c(data_types$Project$ProjectType$lh, data_types$Project$ProjectType$ph)
      | PersonalID %in% PID_homeless
    ) |>
    dplyr::filter(ProjectType != 12) |>
    dplyr::filter(!(ProjectType == 4 & grepl("YHDP", ProjectName, ignore.case = TRUE))) |>
    dplyr::select(
      PersonalID,
      UniqueID,
      ProjectName,
      ProjectType,
      HouseholdID,
      EnrollmentID,
      RelationshipToHoH,
      VeteranStatus,
      EntryDate,
      AgeAtEntry
    )

  # filter so that result only shows client's most secure project type
  co_currently_homeless <- co_currently_homeless |>
    dplyr::group_by(UniqueID) |>
    dplyr::arrange(match(ProjectType, importance_ranking$ProjectType), .by_group = TRUE) |>
    dplyr::slice(1) |>
    dplyr::ungroup()


  # Check Whether Each Client Has Income ---------------------------------

  # getting income-related data and data collection stages. this will balloon
  # out the number of rows per client, listing each yes/no update, then, using
  # DateCreated, it picks out the most recent answer, keeping only that one

  income_data <- co_currently_homeless |>
    dplyr::left_join(
      dplyr::select(
        IncomeBenefits,
        PersonalID,
        EnrollmentID,
        IncomeFromAnySource,
        DateCreated,
        DataCollectionStage
      ),
      by = c("PersonalID", "EnrollmentID")
    ) |>
    dplyr::mutate(IncomeFromAnySource = dplyr::if_else(
      is.na(IncomeFromAnySource),
      dplyr::if_else(AgeAtEntry >= 18L |
                       is.na(AgeAtEntry), 99L, 0L),
      IncomeFromAnySource
    )) |>
    dplyr::group_by(PersonalID, EnrollmentID) |>
    dplyr::arrange(dplyr::desc(DateCreated)) |>
    dplyr::slice(1L) |>
    dplyr::ungroup() |>
    dplyr::select(PersonalID,
                  EnrollmentID,
                  IncomeFromAnySource)

  # Check Whether Each Client Has Any Indication of Disability ------------

  # this checks the enrollment's 1.3 and 4.02 records to catch potential
  # disabling conditions that may be used to determine PSH eligibility but
  # were not reported in 3.08. If any of these three data elements (1.3,
  # 4.02, 3.08) suggest the presence of a disabling condition, this section
  # flags that enrollment as belonging to a disabled client. Otherwise,
  # the enrollment is marked not disabled.


  extended_disability <- co_currently_homeless |>
    dplyr::left_join(
      dplyr::select(
        Disabilities,
        EnrollmentID,
        DisabilityResponse,
        IndefiniteAndImpairs),
      by = c("EnrollmentID"))  |>
    dplyr::group_by(EnrollmentID) |>
    dplyr::mutate(
      D_Disability = dplyr::if_else(DisabilityResponse == 1 &
                                      IndefiniteAndImpairs != 0, 1, 0),
      D_Disability = max(D_Disability)
    ) |>
    dplyr::select(EnrollmentID, D_Disability) |>
    dplyr::left_join(
      dplyr::select(
        IncomeBenefits,
        EnrollmentID,
        SSDI,
        VADisabilityService,
        VADisabilityNonService,
        PrivateDisability),
      by = c("EnrollmentID")) |>
    dplyr::mutate(
      I_Disability = dplyr::if_else(
        SSDI == 1 |
          VADisabilityService == 1 |
          VADisabilityNonService == 1 |
          PrivateDisability == 1,
        1,
        0
      ),
      I_Disability = max(I_Disability)
    ) |>
    dplyr::select(EnrollmentID, D_Disability, I_Disability) |>
    dplyr::ungroup() |>
    dplyr::distinct() |>
    dplyr::left_join(
      dplyr::select(Enrollment_extra_Client_Exit_HH_CL_AaE,
                    EnrollmentID,
                    DisablingCondition),
      by = c("EnrollmentID")) |>
    dplyr::mutate(
      any_disability = dplyr::case_when(
        D_Disability == 1 |
          I_Disability == 1 |
          DisablingCondition == 1 ~ 1,
        TRUE ~ 0
      )
    ) |>
    dplyr::select(EnrollmentID, any_disability)

  # adding household aggregations into the full client list
  prioritization <- co_currently_homeless |>
    dplyr::left_join(income_data,
                     by = c("PersonalID", "EnrollmentID")) |>
    dplyr::left_join(household_size,
                     by = c("PersonalID", "EnrollmentID")) |>
    dplyr::left_join(extended_disability, by = "EnrollmentID") |>
    dplyr::left_join(
      dplyr::select(
        Enrollment_extra_Client_Exit_HH_CL_AaE,
        EnrollmentID,
        PersonalID,
        HouseholdID,
        LivingSituation,
        DateToStreetESSH,
        TimesHomelessPastThreeYears,
        ExitAdjust,
        MoveInDateAdjust,
        MoveInDate,
        DateToStreetESSH,
        TimesHomelessPastThreeYears,
        MonthsHomelessPastThreeYears,
        DisablingCondition
      ),
      by = c("PersonalID",
             "EnrollmentID",
             "HouseholdID")
    ) |>
    dplyr::mutate(SinglyChronic =
                    dplyr::if_else(((
                      DateToStreetESSH + lubridate::years(1) <= EntryDate &
                        !is.na(DateToStreetESSH)
                    ) |
                      (
                        MonthsHomelessPastThreeYears %in% c(112, 113) &
                          TimesHomelessPastThreeYears == 4 &
                          !is.na(MonthsHomelessPastThreeYears) &
                          !is.na(TimesHomelessPastThreeYears)
                      )
                    ) &
                      DisablingCondition == 1 &
                      !is.na(DisablingCondition), 1, 0)) |>
    dplyr::group_by(PersonalID) |>
    dplyr::mutate(SinglyChronic = max(SinglyChronic)) |>
    dplyr::ungroup() |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(
      IncomeInHH = max(
        dplyr::if_else(IncomeFromAnySource == 1, 100L, IncomeFromAnySource)
      ),
      IncomeInHH = dplyr::if_else(IncomeInHH == 100, 1L, IncomeInHH),
      DisabilityInHH = max(dplyr::if_else(any_disability == 1, 1, 0)),
      ChronicStatus = dplyr::if_else(max(SinglyChronic) == 1, "Chronic", "Not Chronic"),
      MaxMD = valid_max(MoveInDateAdjust),
      MaxED = valid_max(EntryDate),
      NewlyHomeless = EntryDate > MaxMD
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      "PersonalID",
      "UniqueID",
      "ProjectName",
      "ProjectType",
      "HouseholdID",
      "EnrollmentID",
      "MaxED",
      "MaxMD",
      "NewlyHomeless",
      "RelationshipToHoH",
      "VeteranStatus",
      "EntryDate",
      "MoveInDateAdjust",
      "MoveInDate",
      "AgeAtEntry",
      "DisablingCondition",
      "HouseholdSize",
      "IncomeInHH",
      "DisabilityInHH",
      "ChronicStatus"
    )


  # correcting for bad hh data (while also flagging it) ---------------------


  # what household ids exist in the data?
  ALL_HHIDs <- prioritization |> dplyr::select(HouseholdID) |> unique()
  .singles <- prioritization |>
    dplyr::group_by(HouseholdID) |>
    dplyr::summarise(N = dplyr::n(), .groups = "drop") |>
    dplyr::filter(N == 1) |>
    dplyr::pull(HouseholdID)
  # marking who is a hoh (accounts for singles not marked as hohs in the data)
  prioritization <- prioritization |>
    dplyr::mutate(
      RelationshipToHoH = dplyr::if_else(is.na(RelationshipToHoH), 99L, RelationshipToHoH),
      hoh = dplyr::if_else(HouseholdID %in% .singles |
                             RelationshipToHoH == 1, 1L, 0L))

  # what household ids exist if we only count those with a hoh?
  HHIDs_in_current_logic <- prioritization |>
    dplyr::filter(hoh == 1) |>
    dplyr::select(HouseholdID) |>
    unique()

  # which hh ids did not have a hoh?
  HHIDs_with_bad_dq <-
    dplyr::anti_join(ALL_HHIDs, HHIDs_in_current_logic,
                     by = "HouseholdID")

  # what household ids have multiple hohs?
  mult_hohs <- prioritization |>
    dplyr::group_by(HouseholdID) |>
    dplyr::summarise(hohs = sum(hoh)) |>
    dplyr::filter(hohs > 1) |>
    dplyr::select(HouseholdID)

  # give me ALL household ids with some sort of problem
  HHIDs_with_bad_dq <- rbind(HHIDs_with_bad_dq, mult_hohs)

  # let's see those same household ids but with all the needed columns
  HHIDs_with_bad_dq <-
    dplyr::left_join(HHIDs_with_bad_dq, prioritization, by = "HouseholdID")

  rm(ALL_HHIDs, HHIDs_in_current_logic, mult_hohs, .singles)

  # assigning hoh status to the oldest person in the hh
  Adjusted_HoHs <- HHIDs_with_bad_dq |>
    dplyr::group_by(HouseholdID) |>
    dplyr::arrange(dplyr::desc(AgeAtEntry)) |> # picking oldest hh member
    dplyr::slice(1L) |>
    dplyr::mutate(correctedhoh = 1L) |>
    dplyr::select(HouseholdID, PersonalID, EnrollmentID, correctedhoh) |>
    dplyr::ungroup()

  # merging the "corrected" hohs back into the main dataset with a flag, then
  # correcting the RelationshipToHoH
  hohs <- prioritization |>
    dplyr::left_join(Adjusted_HoHs,
                     by = c("HouseholdID", "PersonalID", "EnrollmentID")) |>
    dplyr::mutate(RelationshipToHoH = dplyr::if_else(correctedhoh == 1L, 1L, RelationshipToHoH)) |>
    dplyr::select(PersonalID, HouseholdID, correctedhoh)


  prioritization <- prioritization |>
    dplyr::left_join(hohs, by = c("HouseholdID", "PersonalID")) |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(correctedhoh = dplyr::if_else(is.na(correctedhoh), 0L, 1L),
                  HH_DQ_Issue = as.logical(max(correctedhoh))) |>
    dplyr::ungroup() |>
    dplyr::filter(correctedhoh == 1 | RelationshipToHoH == 1 | NewlyHomeless)


  # Adding in TAY, County, PHTrack ----------------------

  # getting whatever data's needed from the Enrollment data frame, creating
  # columns that tell us something about each household and some that are about
  # each client

  prioritization <- prioritization |>
    dplyr::left_join(
      dplyr::distinct(
        Enrollment_extra_Client_Exit_HH_CL_AaE,
        PersonalID,
        HouseholdID,
        CountyServed,
        PHTrack,
        ExpectedPHDate,
        Situation,
        HousingStatus
      ),
      by = c("PersonalID", "HouseholdID")
    ) |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(
      TAY = max(AgeAtEntry) < 25 & max(AgeAtEntry) >= 16,
      PHTrack = dplyr::if_else(
        !is.na(PHTrack) &
          !is.na(ExpectedPHDate) &
          ExpectedPHDate >= lubridate::today(), PHTrack, NA_character_)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-AgeAtEntry)


  # County Guessing ---------------------------------------------------------

  # replacing non-Unsheltered-Provider missings with County of the provider
  prioritization <- prioritization |>
    dplyr::left_join(Project |>
                       dplyr::filter(!OperatingEndDate < Sys.Date() | is.na(OperatingEndDate)) |>
                       dplyr::select(ProjectName, ProjectCounty), by = "ProjectName") |>
    dplyr::mutate(
      CountyGuessed = is.na(CountyServed),
      CountyServed = dplyr::if_else(
        CountyGuessed &
          ProjectName != "Unsheltered Clients - OUTREACH",
        ProjectCounty,
        CountyServed
      ),
      ProjectCounty = NULL,
      CountyServed = dplyr::if_else(is.na(CountyServed), "MISSING County", CountyServed)
    )


  # Add in Score ------------------------------------------------------------
  # taking the most recent score on the client, but this score cannot be over a
  # year old.
  prioritization <- prioritization |>
    dplyr::left_join(
      Scores |>
        dplyr::mutate(PersonalID = as.character(PersonalID),
                      ScoreDate = as.Date(ScoreDate)) |>
        dplyr::filter(ScoreDate > lubridate::today() - lubridate::years(1)) |>
        dplyr::group_by(PersonalID) |>
        dplyr::arrange(dplyr::desc(ScoreDate)) |>
        dplyr::slice(1L) |>
        dplyr::ungroup() |>
        dplyr::select(-ScoreDate, -UniqueID),
      by = "PersonalID"
    )


  # Add Additional Chronic Statuses ---------------------------------------------


  # adds current days in ES or SH projects to days homeless prior to entry and if
  # it adds up to 365 or more, it marks the client as AgedIn
  agedIntoChronicity <- prioritization |>
    dplyr::left_join(
      Enrollment_extra_Client_Exit_HH_CL_AaE |>
        dplyr::select(
          EnrollmentID,
          PersonalID,
          HouseholdID,
          LivingSituation,
          DateToStreetESSH,
          TimesHomelessPastThreeYears,
          ExitAdjust,
          MonthsHomelessPastThreeYears
        ),
      by = c("PersonalID",
             "EnrollmentID",
             "HouseholdID")
    ) |>
    dplyr::mutate(
      DaysHomelessInProject = difftime(ExitAdjust,
                                       EntryDate,
                                       units = "days"),
      DaysHomelessBeforeEntry = difftime(EntryDate,
                                         dplyr::if_else(
                                           is.na(DateToStreetESSH),
                                           EntryDate,
                                           DateToStreetESSH
                                         ),
                                         units = "days"),
      ChronicStatus = dplyr::if_else(
        ProjectType %in% c(0, 1, 4, 8, 14) &
          ChronicStatus == "Not Chronic" &
          DateToStreetESSH + lubridate::days(365) > EntryDate &
          !is.na(DateToStreetESSH) &
          DaysHomelessBeforeEntry + DaysHomelessInProject >= 365,
        "Possibly Chronic",
        ChronicStatus
      )
    ) |>
    dplyr::select(-DaysHomelessInProject,-DaysHomelessBeforeEntry)

  # adds another ChronicStatus of "Nearly Chronic" which catches those hhs with
  # almost enough times and months to qualify as Chronic
  nearly_chronic <- agedIntoChronicity |>
    dplyr::mutate(
      ChronicStatus = dplyr::if_else(
        ChronicStatus == "Not Chronic" &
          ((
            DateToStreetESSH + lubridate::days(365) <= EntryDate &
              !is.na(DateToStreetESSH)
          ) |
            (
              MonthsHomelessPastThreeYears %in% c(110:113) &
                TimesHomelessPastThreeYears%in% c(3, 4) &
                !is.na(MonthsHomelessPastThreeYears) &
                !is.na(TimesHomelessPastThreeYears)
            )
          ) &
          DisablingCondition == 1 &
          !is.na(DisablingCondition),
        "Nearly Chronic",
        ChronicStatus
      )
    )

  prioritization <- prioritization |>
    dplyr::select(-ChronicStatus) |>
    dplyr::left_join(
      dplyr::select(
        nearly_chronic,
        "PersonalID",
        "HouseholdID",
        "EnrollmentID",
        "ChronicStatus",
        "DateToStreetESSH",
        "TimesHomelessPastThreeYears",
        "MonthsHomelessPastThreeYears"
      ),
      by = c("PersonalID", "HouseholdID", "EnrollmentID")
    ) |>
    dplyr::mutate(TimesHomelessPastThreeYears = dplyr::case_when(
      TimesHomelessPastThreeYears == 4 ~ "Four or more times",
      TimesHomelessPastThreeYears == 8 ~ "Client doesn't know",
      TimesHomelessPastThreeYears == 9 ~ "Client prefers not to answer",
      TimesHomelessPastThreeYears == 99 ~ "Data not collected",
      TRUE ~ as.character(TimesHomelessPastThreeYears)
    )) |>
    dplyr::mutate(MonthsHomelessPastThreeYears = dplyr::case_when(
      MonthsHomelessPastThreeYears >= 113 ~ "More than 12 months",
      MonthsHomelessPastThreeYears == 8 ~ "Client doesn't know",
      MonthsHomelessPastThreeYears == 9 ~ "Client prefers not to answer",
      MonthsHomelessPastThreeYears == 99 ~ "Data not collected",
      TRUE ~ as.character(MonthsHomelessPastThreeYears - 100)
    )) |>
    dplyr::mutate(ChronicStatus = dplyr::if_else(
      ChronicStatus == "Chronic", "Possibly Chronic", ChronicStatus
    )) |>
    dplyr::mutate(ChronicStatus = factor(
      ChronicStatus,
      levels = c("Possibly Chronic",
                 "Nearly Chronic",
                 "Not Chronic")
    ))

  # THIS IS WHERE WE'RE SUMMARISING BY HOUSEHOLD (after all the group_bys)
  prioritization <- prioritization |>
    dplyr::mutate(
      HoH_Adjust = dplyr::case_when(HH_DQ_Issue == 1L ~ correctedhoh,
                                    HH_DQ_Issue == 0L ~ hoh)
    ) |>
    dplyr::filter(HoH_Adjust == 1 &
                    is.na(MoveInDateAdjust)) |>
    dplyr::select(-correctedhoh, -RelationshipToHoH, -hoh, -HoH_Adjust)


  # Add Referral Status -----------------------------------------------------

  # thinking maybe it makes the most sense to only look at referrals that have
  # been accepted for the purposes of Prioritization. Because who cares if
  # there's an open referral on a client who needs housing? That doesn't mean
  # anything because we haven't really assigned a meaning to that. But an
  # accepted referral does supposedly mean something, and it would add context
  # to know that a household on this list has been accepted into (if not entered
  # into) another project.

  # also thinking the Refer-to provider should be an RRH or PSH? Maybe? Because
  # referrals to a homeless project wouldn't mean anything on an Active List,
  # right?

  # Referral handling & PTCStatus & Housing Status moved to Load Export since this data is essential in other reports. ----
  # Wed Mar 16 10:37:00 2022


  # Filter Housed and likely housed
  prioritization <- prioritization |>
    dplyr::group_by(PersonalID) |>
    # get the lowest priority (furthest towards housed)
    dplyr::slice_min(HousingStatus, with_ties = FALSE) |>
    dplyr::filter(!HousingStatus %in% c("Housed", "Likely housed")) |>
    dplyr::select( - dplyr::starts_with("R_"))


  # Fleeing DV --------------------------------------------------------------
  prioritization <- prioritization |>
    dplyr::left_join(
      HealthAndDV |>
        # get most recent DV information for those on the list
        dplyr::group_by(PersonalID) |>
        dplyr::arrange(dplyr::desc(InformationDate)) |>
        dplyr::slice(1L) |>
        # pull variables we want
        dplyr::select(EnrollmentID,
                      PersonalID,
                      CurrentlyFleeing,
                      WhenOccurred),
      by = c("EnrollmentID", "PersonalID")
    ) |>
    dplyr::mutate(
      CurrentlyFleeing = dplyr::if_else(is.na(CurrentlyFleeing), 99L, CurrentlyFleeing),
      WhenOccurred = dplyr::if_else(is.na(WhenOccurred), 99L, WhenOccurred),
      CurrentlyFleeing = dplyr::case_when(
        CurrentlyFleeing %in% c(0L, 99L) &
          WhenOccurred %in% c(4L, 8L, 9L, 99L) ~ "No",
        CurrentlyFleeing == 1L |
          WhenOccurred %in% c(1L:3L) ~ "Yes",
        CurrentlyFleeing %in% c(8L, 9L) ~ "Unknown"
      )
    ) |>
    dplyr::select(-WhenOccurred)

  # Clean the House ---------------------------------------------------------
  prioritization <- prioritization |>
    dplyr::mutate(
      dplyr::across(dplyr::all_of(c("VeteranStatus", "DisabilityInHH")), HMIS::hud_translations$`1.8 NoYesReasons for Missing Data`),
      IncomeFromAnySource = HMIS::hud_translations$`1.8 NoYesReasons for Missing Data`(IncomeInHH),
      TAY = dplyr::case_when(TAY == 1 ~ "Yes",
                             TAY == 0 ~ "No",
                             is.na(TAY) ~ "Unknown"),
      ProjectName = dplyr::if_else(
        ProjectName == "Unsheltered Clients - OUTREACH",
        paste("Unsheltered in",
              CountyServed,
              "County"),
        ProjectName
      )
    ) |>
    dplyr::select(-IncomeInHH) |>
    dplyr::ungroup()

  browser()

  HMISdata::upload_hmis_data(prioritization,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "prioritization.parquet", format = "parquet")

  return(prioritization)
}
