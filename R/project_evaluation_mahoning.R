
#' @include 06_Project_Evaluation_utils.R
project_evaluation_mahoning <- function(
  Project,
  Funder,
  Enrollment_extra_Client_Exit_HH_CL_AaE,
  rm_dates) {

    co_clients_served <- HMISdata::load_hmis_parquet("co_clients_served.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_adults_served <- HMISdata::load_hmis_parquet("co_adults_served.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_hohs_served <- HMISdata::load_hmis_parquet("co_hohs_served.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_hohs_entered <- HMISdata::load_hmis_parquet("co_hohs_entered.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_clients_moved_in_leavers <- HMISdata::load_hmis_parquet("co_clients_moved_in_leavers.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_hohs_moved_in_leavers <- HMISdata::load_hmis_parquet("co_hohs_moved_in_leavers.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_adults_moved_in_leavers <- HMISdata::load_hmis_parquet("co_adults_moved_in_leavers.parquet", bucket = "hud.csv-daily", "hmis_output")

  # read scoring rubric from google sheets
  # different scoring rubric for Mahoning (need to make sure this is still the case)

  googlesheets4::gs4_auth(path = "inst/vault/rminor@rminor-333915.iam.gserviceaccount.com.json")
  scoring_rubric <- googlesheets4::read_sheet("1lLsNI8A2E-dDE8O2EHmCP9stSImxZkYJTGx-Oxs1W74",
                                              sheet = "Sheet1",
                                              col_types = c("metric" = "c", "goal_type" = "c", "minimum" = "n", "maximum" = "n",
                                                            "points" = "n") |> paste0(collapse = ""))

  merged_projects <-
    list(
      `Mahoning - Help Network - PSH - Combined` = list(c("Help Network - Shelter Plus Care - PSH"), c("PSH Rental Assistance Program - PSH")),
      `Mahoning - Samaritan Housing PRA` = list(c("Homeless Solutions SRO II"), c("Samaritan Housing PRA"))
    )

  merged_projects <- purrr::map(merged_projects, ~{
    reg <- purrr::map(.x, ~UU::regex_op(.x, "&"))
    idx <- purrr::map_dbl(reg, ~stringr::str_which(Project$ProjectName, .x))
    idx <- unlist(idx)
    
    list(ProjectName = Project$ProjectName[idx],
         ProjectID = Project$ProjectID[idx])
  })
  .merged <- rlang::set_names(purrr::map(merged_projects, "ProjectID") |> purrr::flatten_chr(), purrr::map(merged_projects, "ProjectName") |> purrr::flatten_chr())

  # consolidated projects
  pe_coc_funded <- Funder %>%
    dplyr::filter(((StartDate <= rm_dates$hc$project_eval_end &(is.na(EndDate) | EndDate >= rm_dates$hc$project_eval_end)
    ))) |>
    dplyr::select(ProjectID, Funder, StartDate, EndDate) |>
    dplyr::left_join(Project[c("ProjectID",
                               "ProjectName",
                               "ProjectType",
                               "HMISParticipationType",
                               "ProjectRegion")], by = "ProjectID") |>
    # dplyr::filter(HMISParticipatingProject == 1 &
    #                 ProjectRegion != 0) %>%
    dplyr::select(ProjectType,
                  ProjectName,
                  ProjectID,
                  HMISParticipationType,
                  ProjectRegion,
                  StartDate,
                  EndDate) |>
    dplyr::mutate(AltProjectName = merge_projects(ProjectName, merged_projects),
                  AltProjectID = merge_projects(ProjectID, merged_projects)) |>
    dplyr::filter(stringr::str_starts(AltProjectName, "Mahoning")) |>
    dplyr::filter(AltProjectName %in%
                    c("Mahoning - Beatitude House - Permanent Supportive Housing Program - PSH",
                      "Mahoning - Meridian Services - Phoenix Court - PSH",
                      "Mahoning - MCMHRB - Shelter Plus Care - PSH",
                      # "Mahoning - Meridian Services - Samaritan Housing PRA - PSH",
                      # "Mahoning - Meridian Services - Homeless Solutions SRO II - PSH",
                      "Mahoning - Ursuline Center - Merici - PSH",
                      "Mahoning - YWCA Permanent Housing for Disabled Families - PSH",
                      "Mahoning - YWCA Scattered Site Housing II - PSH",
                      "Mahoning - Help Network - PSH - Combined",
                      "Mahoning - Samaritan Housing PRA"
                    ))


  vars <- list()
  vars$prep <- c(
    "PersonalID",
    "UniqueID",
    "ProjectType",
    "AltProjectID",
    "VeteranStatus",
    "EnrollmentID",
    "AltProjectName",
    "EntryDate",
    "HouseholdID",
    "RelationshipToHoH",
    "LivingSituation",
    "LengthOfStay",
    "LOSUnderThreshold",
    "PreviousStreetESSH",
    "DateToStreetESSH",
    "TimesHomelessPastThreeYears",
    "AgeAtEntry",
    "MonthsHomelessPastThreeYears",
    "DisablingCondition",
    "MoveInDate",
    "MoveInDateAdjust",
    "ExitDate",
    "Destination",
    "DestinationSubsidyType",
    "EntryAdjust",
    "ExitAdjust"
  )

  vars$we_want <- c(
    "ProjectType",
    "AltProjectName",
    "PersonalID",
    "UniqueID",
    "EnrollmentID",
    "HouseholdID",
    "EntryDate",
    "MoveInDateAdjust",
    "ExitDate",
    "MeetsObjective"
  )

  # Project Evaluation cohorts ----------------------------------------------

  # pe_[cohort]: uses cohort objects to narrow down data to coc-funded projects'
  # data to the 'vars$prep', then dedupes in case there are multiple stays in
  # that project during the date range.

  # clients served during date range

  # no dupes w/in a project
  pe <- list()
  pe$ClientsServed <- peval_filter_select(co_clients_served, vars = vars$prep,  served = TRUE)
  # several measures will use this
  # Checking for deceased hohs for points adjustments


  hoh_exits_to_deceased <- pe$ClientsServed %>%
    HMIS::exited_between(rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end) |>
    dplyr::filter(Destination == 24 &
                    RelationshipToHoH == 1) %>%
    dplyr::group_by(AltProjectID) %>%
    dplyr::summarise(HoHDeaths = dplyr::n(), .groups = "drop") %>%
    dplyr::right_join(pe_coc_funded["AltProjectID"] %>% unique(), by = "AltProjectID")

  hoh_exits_to_deceased[is.na(hoh_exits_to_deceased)] <- 0

  # Adults who entered during date range

  pe$AdultsEntered <- peval_filter_select(co_adults_served, vars = vars$prep, distinct = FALSE) |>
    dplyr::group_by(HouseholdID) %>%
    dplyr::mutate(HHEntryDate = min(EntryDate)) %>%
    dplyr::ungroup() |>
    HMIS::entered_between(rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end) |>
    dplyr::filter(EntryDate == HHEntryDate) |>
    dplyr::select(-HHEntryDate)


  # counts each client's entry

  ## for vispdat measure

  pe$HoHsEntered <- peval_filter_select(co_hohs_entered, vars = vars$prep, entered = TRUE, distinct = FALSE)

  # for ncb logic
  # Adults who moved in and exited during date range

  pe$AdultsMovedInLeavers <- peval_filter_select(co_adults_moved_in_leavers, vars = vars$prep, stayed = TRUE, exited = TRUE)


  # health insurance
  # Clients who moved in and exited during date range

  pe$ClientsMovedInLeavers <- peval_filter_select(co_clients_moved_in_leavers, vars = vars$prep, stayed = TRUE, exited = TRUE)

  # exits to PH, but needs an added filter of only mover-inners
  # Heads of Household who were served during date range

  pe$HoHsServed <- peval_filter_select(co_hohs_served, vars = vars$prep, served = TRUE)

  pe$HoHsServedLeavers <- peval_filter_select(co_hohs_served, vars = vars$prep, served = TRUE, exited = TRUE)

  # own housing and LoS
  # Heads of Household who moved in and exited during date range

  pe$HoHsMovedInLeavers <- peval_filter_select(co_hohs_moved_in_leavers, vars = vars$prep, stayed = TRUE, exited = TRUE)


  # Create Validation Summary -----------------------------------------------

  # summary_pe_[cohort] - takes client-level pe_[cohort], calculates # of total
  # clients in the cohort at the alt-project level



  pe_summary_validation_mahoning <- rlang::set_names(pe, paste0("summary_", names(pe))) |>
    purrr::imap(peval_summary, app_env = app_env) |>
    purrr::reduce(dplyr::full_join, by = "AltProjectID") |>
    dplyr::left_join(pe_coc_funded %>%
                       dplyr::select(AltProjectID, ProjectType, AltProjectName) %>%
                       unique(), by = c("AltProjectID")) %>%
    dplyr::left_join(hoh_exits_to_deceased, by = "AltProjectID")

  # joins all summary_pe_[cohort]s into one object so now you have all the cohort
  # totals at the alt-project level






  # Finalizing DQ Flags -----------------------------------------------------

  # calculates how many clients have a qualifying error of whatever type. only
  # returns the providers with any qualifying errors.
  dq_flags_staging <- dq_for_pe %>%
    dplyr::right_join(pe_coc_funded, by = c("ProjectType", "ProjectID", "ProjectName")) %>%
    dplyr::mutate(
      GeneralFlag =
        dplyr::if_else(
          Issue %in% c(
            "Duplicate Entry Exits",
            "Incorrect Entry Exit Type",
            "Children Only Household",
            "No Head of Household",
            "Too Many Heads of Household",
            "Missing Relationship to Head of Household"
          ),
          1,
          0
        ),
      BenefitsFlag =
        dplyr::if_else(
          Issue %in% c(
            "Non-cash Benefits Missing at Entry",
            "Conflicting Non-cash Benefits yes/no at Entry"
          ),
          1,
          0
        ),
      IncomeFlag =
        dplyr::if_else(
          Issue %in% c("Income Missing at Entry",
                       "Conflicting Income yes/no at Entry"),
          1,
          0
        ),
      LoTHFlag =
        dplyr::if_else(
          Issue %in% c("Missing Prior Living Situation",
                       "Missing Months or Times Homeless",
                       "Incomplete Living Situation Data"),
          1,
          0
        )
    ) %>%
    dplyr::select(AltProjectName,
                  PersonalID,
                  HouseholdID,
                  GeneralFlag,
                  BenefitsFlag,
                  IncomeFlag,
                  LoTHFlag) %>%
    dplyr::filter(
      GeneralFlag + BenefitsFlag + IncomeFlag + LoTHFlag > 0
    ) %>%
    dplyr::group_by(AltProjectName) %>%
    dplyr::summarise(GeneralFlagTotal = sum(GeneralFlag),
                     BenefitsFlagTotal = sum(BenefitsFlag),
                     IncomeFlagTotal = sum(IncomeFlag),
                     LoTHFlagTotal = sum(LoTHFlag))

  # calculates whether the # of errors of whatever type actually throws a flag.
  # includes all alt-projects regardless of if they have errors

  data_quality_flags <- pe_summary_validation_mahoning %>%
    dplyr::left_join(dq_flags_staging, by = "AltProjectName") %>%
    dplyr::mutate(General_DQ = dplyr::if_else(GeneralFlagTotal/ClientsServed >= .02, 1, 0),
                  Benefits_DQ = dplyr::if_else(BenefitsFlagTotal/AdultsEntered >= .02, 1, 0),
                  Income_DQ = dplyr::if_else(IncomeFlagTotal/AdultsEntered >= .02, 1, 0),
                  LoTH_DQ = dplyr::if_else(LoTHFlagTotal/HoHsServed >= .02, 1, 0))

  data_quality_flags[is.na(data_quality_flags)] <- 0

  # writing out a file to help notify flagged projects toward end of process

  # TODO Automation email drafts to the right set of users (need to filter COHHIO_admin_user_ids)
  # Retrieve AgencyID, get all attached UserIDs, Send email to those Users.


  pe_users_info <- data_quality_flags %>%
    dplyr::filter(GeneralFlagTotal > 0 |
                    BenefitsFlagTotal > 0 |
                    IncomeFlagTotal > 0 |
                    LoTHFlagTotal > 0) %>%
    dplyr::left_join(pe_coc_funded %>%
                       dplyr::distinct(ProjectID, AltProjectID), by = "AltProjectID") %>%
    dplyr::mutate(AltProjectID = dplyr::if_else(is.na(ProjectID), AltProjectID, ProjectID)) %>%
    dplyr::left_join(User_extras |>
                       dplyr::mutate(ProjectID = as.character(ProjectID)) |>
                       dplyr::filter(!is.na(ProjectID) & Deleted == "No") |>
                       dplyr::select(UserID, ProjectID), by = "ProjectID")

  # this file ^^ is used by Reports/CoC_Competition/Notify_DQ.Rmd to produce
  # emails to all users attached to any of the providers with DQ flags.

  # displays flags thrown at the alt-project level

  data_quality_flags <- data_quality_flags %>%
    dplyr::select(AltProjectName, dplyr::ends_with("DQ"))

  # CoC Scoring -------------------------------------------------------------

  # NOTE Dependency needs to be fetched from cloud location
  coc_scoring <- arrow::read_feather(file.path(dirs$public, "coc_scoring.feather")) |>
    dplyr::mutate(DateReceivedPPDocs = as.Date(DateReceivedPPDocs, origin = "1899-12-30"),
                  ProjectID = as.character(ProjectID))



  summary_pe_coc_scoring <- dplyr::left_join(pe_coc_funded, coc_scoring, by = c("ProjectID")) %>%
    dplyr::select(
      ProjectType,
      ProjectID,
      AltProjectID,
      AltProjectName,
      DateReceivedPPDocs,
      HousingFirstScore,
      ChronicPrioritizationScore,
      PrioritizationWorkgroupScore
    ) %>%
    dplyr::filter(!ProjectID %in% purrr::map_chr(merged_projects, ~.x[[2]][2])) %>%
    dplyr::mutate(
      Submission_Math = peval_math(DateReceivedPPDocs, rm_dates$hc$project_eval_docs_due),
      PrioritizationWorkgroupPossible = 5,
      PrioritizationWorkgroupScore = tidyr::replace_na(PrioritizationWorkgroupScore, 0),
      HousingFirstPossible = 15,
      HousingFirstDQ = dplyr::case_when(
        DateReceivedPPDocs <= rm_dates$hc$project_eval_docs_due &
          is.na(HousingFirstScore) ~ 3,
        is.na(DateReceivedPPDocs) &
          is.na(HousingFirstScore) ~ 2,
        is.na(DateReceivedPPDocs) &
          !is.na(HousingFirstScore) ~ 4,
        DateReceivedPPDocs > rm_dates$hc$project_eval_docs_due ~ 5
      ),
      HousingFirstScore = dplyr::case_when(
        is.na(DateReceivedPPDocs) |
          is.na(HousingFirstScore) ~ -10,
        DateReceivedPPDocs > rm_dates$hc$project_eval_docs_due ~ -10,
        DateReceivedPPDocs <= rm_dates$hc$project_eval_docs_due ~ HousingFirstScore
      ),
      ChronicPrioritizationDQ = dplyr::case_when(
        DateReceivedPPDocs <= rm_dates$hc$project_eval_docs_due &
          is.na(ChronicPrioritizationScore) ~ 3,
        is.na(DateReceivedPPDocs) &
          is.na(ChronicPrioritizationScore) ~ 2,
        is.na(DateReceivedPPDocs) &
          !is.na(ChronicPrioritizationScore) ~ 4,
        DateReceivedPPDocs > rm_dates$hc$project_eval_docs_due ~ 5
      ),
      ChronicPrioritizationPossible = dplyr::if_else(ProjectType == 3, 10, NA),
      ChronicPrioritizationScore =
        dplyr::case_when(
          DateReceivedPPDocs <= rm_dates$hc$project_eval_docs_due &
            ProjectType == 3 &
            !is.na(ChronicPrioritizationScore) ~ ChronicPrioritizationScore,
          is.na(DateReceivedPPDocs) &
            ProjectType == 3 &
            is.na(ChronicPrioritizationScore) ~ -5,
          DateReceivedPPDocs > rm_dates$hc$project_eval_docs_due &
            ProjectType == 3 ~ -5
        )
    )

  pt_adjustments_after_freeze <- summary_pe_coc_scoring %>%
    dplyr::mutate(
      PrioritizationWorkgroupScore = dplyr::case_when(
        AltProjectID %in% c(1088, 730) ~ 1,
        TRUE ~ PrioritizationWorkgroupScore
      ),
      ChronicPrioritizationScore = dplyr::case_when(
        AltProjectID == 1673 ~ 6,
        AltProjectID == 719 ~ 10,
        TRUE ~ ChronicPrioritizationScore
      )
    )

  summary_pe_coc_scoring <- pt_adjustments_after_freeze

  # 2 = Documents not yet received
  # 3 = Docs received, not yet scored
  # 4 = CoC Error
  # 5 = Docs received past the due date

  summary_pe <- list()
  # Housing Stability: Exits to PH ------------------------------------------

  # pe_[measure] - client-level dataset of all clients counted in the measure
  # along with whether each one meets the objective
  # summary_pe_[measure] - uses pe_[measure] to smush to alt-project level and
  # adds a score

  # PSH (includes stayers tho), TH, SH, RRH

  pe$ExitsToPHMahoning <- pe$HoHsServed %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    (\(x) {
      dplyr::filter(
        x,
        (ProjectType %in% c(2, 8, 13) &
           HMIS::exited_between(x, rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end, lgl = TRUE)) |
          ProjectType == 3
      )
    })() |> # filtering out non-PSH stayers
    dplyr::mutate(
      DestinationGroup = dplyr::case_when(
        is.na(Destination) | ExitAdjust > rm_dates$hc$project_eval_end ~
          "Still in Program at Report End Date",
        Destination %in% destinations$temp ~ "Temporary",
        Destination %in% destinations$perm ~ "Permanent",
        Destination %in% destinations$institutional ~ "Institutional",
        Destination == 24 ~ "Deceased (not counted)",
        Destination %in% destinations$other ~ "Other"
      ),
      ExitsToPHDQ = dplyr::case_when(
        General_DQ == 1 ~ 1,
        TRUE ~ 0
      ),
      MeetsObjective =
        dplyr::case_when(
          DestinationGroup == "Permanent" |
            (ProjectType == 3 &
               DestinationGroup == "Still in Program at Report End Date") ~ 1,
          TRUE ~ 0
        )
    ) %>%
    dplyr::select(dplyr::all_of(vars$we_want), ExitsToPHDQ, Destination, DestinationGroup)

  summary_pe$ExitsToPHMahoning <- pe$ExitsToPHMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, ExitsToPHDQ) %>%
    dplyr::summarise(ExitsToPH = sum(MeetsObjective), .groups = "drop") %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      ExitsToPHCohort = dplyr::if_else(ProjectType == 3, "HoHsServed", "HoHsServedLeavers"),
      HoHsServedLeavers = HoHsServedLeavers - HoHDeaths,
      HoHsServed = HoHsServed - HoHDeaths,
      ExitsToPH = dplyr::if_else(is.na(ExitsToPH), 0, ExitsToPH),
      ExitsToPHPercent = dplyr::if_else(
        ProjectType == 3,
        ExitsToPH / HoHsServed,
        ExitsToPH / HoHsServedLeavers
      ),
      ExitsToPHPercentJoin = dplyr::if_else(is.na(ExitsToPHPercent), 0, ExitsToPHPercent)) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "exits_to_ph"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(ExitsToPHPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= ExitsToPHPercentJoin &
                                   maximum > ExitsToPHPercentJoin,
                                 minimum < ExitsToPHPercentJoin &
                                   maximum >= ExitsToPHPercentJoin)) %>%
    dplyr::mutate(ExitsToPHMath = dplyr::case_when(
      ProjectType == 3 & HoHsServed != 0 ~
        paste(
          ExitsToPH,
          "exits to permanent housing or retention in PSH /",
          HoHsServed,
          "heads of household =",
          scales::percent(ExitsToPHPercent, accuracy = 1)
        ),
      ProjectType != 3 & HoHsServedLeavers != 0 ~
        paste(
          ExitsToPH,
          "exits to permanent housing /",
          HoHsServedLeavers,
          "heads of household leavers =",
          scales::percent(ExitsToPHPercent, accuracy = 1)
        )
    ),
    ExitsToPHPoints = dplyr::if_else(
      (ProjectType == 3 &
         HoHsServed == 0) |
        (ProjectType != 3 &
           HoHsServedLeavers == 0),
      ExitsToPHPossible, points
    ),
    ExitsToPHPoints = dplyr::if_else(
      ExitsToPHDQ == 0 | is.na(ExitsToPHDQ),
      ExitsToPHPoints,
      0
    )
    ) %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      ExitsToPH,
      ExitsToPHMath,
      ExitsToPHPercent,
      ExitsToPHPoints,
      ExitsToPHPossible,
      ExitsToPHDQ,
      ExitsToPHCohort
    )

  # # Housing Stability: Moved into Own Housing -------------------------------
  # # RRH, PSH

  pe$OwnHousingMahoning <- pe$HoHsMovedInLeavers %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::filter(ProjectType != 2) %>%
    dplyr::mutate(DaysInProject = difftime(MoveInDateAdjust, EntryDate, units = "days")) %>%
    dplyr::mutate(
      MeetsObjective = dplyr::case_when(
        ((Destination %in% c(3, 410:411, 421) | DestinationSubsidyType %in% c(419:420, 428, 431, 433:434)) &
           lubridate::ymd(ExitAdjust) <= lubridate::ymd(rm_dates$hc$project_eval_end)) ~ 1,
        TRUE ~ 0
      ),
      OwnHousingDQ = dplyr::case_when(
        General_DQ == 1 ~ 1,
        TRUE ~ 0
      ),
      DestinationGroup = dplyr::case_when(
        is.na(Destination) | lubridate::ymd(ExitAdjust) > lubridate::ymd(rm_dates$hc$project_eval_end) ~
          "Still in Program at Report End Date",
        Destination %in% c(101, 302, 312, 313, 314, 116, 118, 327) ~ "Temporary", # 1, 2, 12, 13, 14, 16, 18, 27
        (Destination %in% c(410:411) | DestinationSubsidyType %in% c(419:421, 428, 431, 433:434)) ~ "Household's Own Housing", # 3, 10:11, 19:21, 28, 31, 33:34
        Destination %in% c(422:423) ~ "Shared Housing", # 22, 23
        Destination %in% c(204:207, 215, 225, 327, 426, 329) ~ "Institutional",
        Destination %in% c(8, 9, 17, 30, 99, 37) ~ "Other",
        Destination == 24 ~ "Deceased"
      ),
      PersonalID = as.character(PersonalID)
    ) %>%
    dplyr::select(dplyr::all_of(vars$we_want),
                  OwnHousingDQ, Destination, DestinationGroup, DaysInProject)

  summary_pe$OwnHousingMahoning <- pe$OwnHousingMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, OwnHousingDQ) %>%
    dplyr::summarise(OwnHousing = sum(MeetsObjective),
                     AverageDays = as.numeric(mean(DaysInProject[OwnHousing > 0], na.rm = TRUE))) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      AverageDaysJoin = dplyr::if_else(is.na(AverageDays), 0, AverageDays)
    ) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "own_housing"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(OwnHousingPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "min",
                                 minimum <= AverageDaysJoin &
                                   maximum > AverageDaysJoin,
                                 minimum < AverageDaysJoin &
                                   maximum >= AverageDaysJoin)) %>%
    dplyr::mutate(
      OwnHousingMath = dplyr::case_when(
        HoHsMovedInLeavers == 0 ~ "All points granted because this project had 0 Heads of Household Leavers who Moved into Housing",
        HoHsMovedInLeavers != 0 & ProjectType != 2 ~ paste(as.integer(AverageDaysJoin), "average days"),
        TRUE ~ ""
      ),
      OwnHousingPoints = dplyr::if_else(
        HoHsMovedInLeavers == 0 & ProjectType != 2, OwnHousingPossible, points
      ),
      OwnHousingPoints = dplyr::case_when(OwnHousingDQ == 1 ~ 0,
                                          is.na(OwnHousingDQ) |
                                            OwnHousingDQ == 0 ~ OwnHousingPoints),
      OwnHousingPoints = dplyr::if_else(is.na(OwnHousingPoints), 0, OwnHousingPoints),
      OwnHousingPoints = dplyr::if_else(OwnHousing == 0, 0, OwnHousingPoints),
      OwnHousingPossible = dplyr::if_else(ProjectType != 2, 5, NA),
      OwnHousingCohort = "HoHsMovedInLeavers"
    ) %>%
    dplyr::select(ProjectType,
                  AltProjectName,
                  OwnHousingCohort,
                  OwnHousing,
                  OwnHousingMath,
                  OwnHousingPoints,
                  OwnHousingPossible,
                  OwnHousingDQ)


  # Accessing Mainstream Resources: Benefits -----------------------------------
  # PSH, TH, SH, RRH

  pe$BenefitsAtExitMahoning <- pe$AdultsMovedInLeavers %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) %>%
    dplyr::select(
      PersonalID,
      UniqueID,
      AltProjectName,
      EnrollmentID,
      ProjectType,
      HouseholdID,
      RelationshipToHoH,
      VeteranStatus,
      EntryDate,
      MoveInDateAdjust,
      AgeAtEntry,
      ExitDate,
      ExitAdjust,
      BenefitsFromAnySource,
      InsuranceFromAnySource,
      DataCollectionStage,
      General_DQ,
      Benefits_DQ
    ) %>%
    dplyr::filter(DataCollectionStage == 3) %>%
    dplyr::mutate(
      MeetsObjective =
        dplyr::case_when(
          (BenefitsFromAnySource == 1 |
             InsuranceFromAnySource == 1) ~ 1,
          TRUE ~ 0
        ),
      BenefitsAtExitDQ = dplyr::if_else(General_DQ == 1 |
                                          Benefits_DQ == 1, 1, 0)
    ) %>%
    dplyr::select(
      dplyr::all_of(vars$we_want),
      BenefitsAtExitDQ,
      BenefitsFromAnySource,
      InsuranceFromAnySource
    )

  summary_pe$BenefitsAtExitMahoning <- pe$BenefitsAtExitMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, BenefitsAtExitDQ) %>%
    dplyr::summarise(BenefitsAtExit = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      BenefitsAtExit = dplyr::if_else(is.na(BenefitsAtExit), 0, BenefitsAtExit),
      BenefitsAtExitPercent = BenefitsAtExit / AdultsMovedInLeavers,
      BenefitsAtExitPercentJoin = dplyr::if_else(is.na(BenefitsAtExitPercent), 0, BenefitsAtExitPercent)) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "benefits_at_exit"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(BenefitsAtExitPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= BenefitsAtExitPercentJoin &
                                   maximum > BenefitsAtExitPercentJoin,
                                 minimum < BenefitsAtExitPercentJoin &
                                   maximum >= BenefitsAtExitPercentJoin)) %>%
    dplyr::mutate(BenefitsAtExitMath = dplyr::if_else(
      AdultsMovedInLeavers == 0,
      "All points granted because this project had no adult leavers who moved into the project's housing",
      paste(
        BenefitsAtExit,
        "exited with benefits or health insurance /",
        AdultsMovedInLeavers,
        "adult leavers who moved into the project's housing =",
        scales::percent(BenefitsAtExitPercent, accuracy = 1)
      )
    ),
    BenefitsAtExitDQ = dplyr::if_else(is.na(BenefitsAtExitDQ), 0, BenefitsAtExitDQ),
    BenefitsAtExitPoints = dplyr::case_when(
      AdultsMovedInLeavers == 0 ~ BenefitsAtExitPossible,
      TRUE ~ points
    ),
    BenefitsAtExitPoints = dplyr::case_when(
      BenefitsAtExitDQ == 1 ~ 0,
      is.na(BenefitsAtExitDQ) |
        BenefitsAtExitDQ == 0 ~ BenefitsAtExitPoints
    ),
    BenefitsAtExitCohort = "AdultsMovedInLeavers"
    ) %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      BenefitsAtExitCohort,
      BenefitsAtExit,
      BenefitsAtExitMath,
      BenefitsAtExitPercent,
      BenefitsAtExitPoints,
      BenefitsAtExitPossible,
      BenefitsAtExitDQ
    )

  # Accessing Mainstream Resources: Increase Total Income -------------------
  # PSH, TH, SH, RRH

  income_staging2 <-  pe$AdultsMovedInLeavers %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) %>%
    dplyr::select(PersonalID,
                  EnrollmentID,
                  EntryDate,
                  ExitDate,
                  TotalMonthlyIncome,
                  DateCreated,
                  DataCollectionStage) %>%
    dplyr::mutate(
      DataCollectionStage = dplyr::case_when(
        DataCollectionStage == 1 ~ "Entry",
        DataCollectionStage == 2 ~ "Update",
        DataCollectionStage == 3 ~ "Exit",
        DataCollectionStage == 5 ~ "Annual"
      )
    )

  income_staging_fixed <- income_staging2 %>%
    dplyr::filter(DataCollectionStage == "Entry")

  income_staging_variable <- income_staging2 %>%
    dplyr::filter(DataCollectionStage %in% c("Update", "Annual", "Exit")) %>%
    dplyr::group_by(EnrollmentID) %>%
    dplyr::mutate(MaxUpdate = max(lubridate::ymd_hms(DateCreated))) %>%
    dplyr::filter(MaxUpdate == DateCreated) %>%
    dplyr::select(-MaxUpdate) %>%
    dplyr::distinct() %>%
    dplyr::ungroup()

  income_staging <- rbind(income_staging_fixed, income_staging_variable) %>%
    dplyr::select(PersonalID, EnrollmentID, TotalMonthlyIncome, DataCollectionStage) %>%
    unique()

  pe$IncreaseIncomeMahoning <- income_staging %>%
    tidyr::pivot_wider(names_from = DataCollectionStage,
                       values_from = TotalMonthlyIncome) %>%
    dplyr::left_join(pe$AdultsMovedInLeavers, by = c("PersonalID", "EnrollmentID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::mutate(
      MostRecentIncome = dplyr::case_when(
        !is.na(Exit) ~ Exit,
        # !is.na(Update) ~ Update,
        # !is.na(Annual) ~ Annual
      ),
      IncomeAtEntry = dplyr::if_else(is.na(Entry), 0, Entry),
      IncomeMostRecent = dplyr::if_else(is.na(MostRecentIncome),
                                        IncomeAtEntry,
                                        MostRecentIncome),
      MeetsObjective = dplyr::case_when(
        IncomeMostRecent > IncomeAtEntry ~ 1,
        IncomeMostRecent <= IncomeAtEntry ~ 0),
      IncreasedIncomeDQ = dplyr::if_else(General_DQ == 1 |
                                           Income_DQ == 1, 1, 0),
      PersonalID = as.character(PersonalID)
    ) %>%
    dplyr::select(
      all_of(vars$we_want),
      IncreasedIncomeDQ,
      IncomeAtEntry,
      IncomeMostRecent
    )

  rm(list = ls(pattern = "income_staging"))

  summary_pe$IncreaseIncomeMahoning <- pe$IncreaseIncomeMahoning |>
    dplyr::group_by(ProjectType, AltProjectName, IncreasedIncomeDQ) |>
    dplyr::summarise(IncreasedIncome = sum(MeetsObjective)) |>
    dplyr::ungroup() |>
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      IncreasedIncome = dplyr::if_else(is.na(IncreasedIncome), 0, IncreasedIncome),
      IncreasedIncomeDQ = dplyr::if_else(is.na(IncreasedIncomeDQ), 0, IncreasedIncomeDQ),
      IncreasedIncomePercent = IncreasedIncome / AdultsMovedInLeavers,
      IncreasedIncomePercentJoin = dplyr::if_else(is.na(IncreasedIncomePercent), 0, IncreasedIncomePercent)
    ) |>
    dplyr::right_join(scoring_rubric |>
                        dplyr::filter(metric == "increase_income"),
                      by = "ProjectType") |>
    dplyr::group_by(ProjectType) |>
    dplyr::mutate(IncreasedIncomePossible = max(points)) |>
    dplyr::ungroup() |>
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= IncreasedIncomePercentJoin &
                                   maximum > IncreasedIncomePercentJoin,
                                 minimum < IncreasedIncomePercentJoin &
                                   maximum >= IncreasedIncomePercentJoin)) %>%
    dplyr::mutate(
      IncreasedIncomeMath = dplyr::if_else(
        AdultsMovedInLeavers != 0,
        paste(
          IncreasedIncome,
          "increased income during their stay /",
          AdultsMovedInLeavers,
          "adults who moved into the project's housing =",
          scales::percent(IncreasedIncomePercent, accuracy = 0.1)
        ),
        "All points granted because 0 adults moved into the project's housing"
      ),
      IncreasedIncomePoints = dplyr::case_when(
        AdultsMovedInLeavers == 0 ~ IncreasedIncomePossible,
        TRUE ~ points),
      IncreasedIncomePoints = dplyr::case_when(
        IncreasedIncomeDQ == 1 ~ 0,
        AdultsMovedInLeavers != 0 &
          (IncreasedIncomeDQ == 0 | is.na(IncreasedIncomeDQ)) ~ IncreasedIncomePoints
      ),
      IncreasedIncomeCohort = "AdultsMovedInLeavers"
    ) |>
    dplyr::select(
      ProjectType,
      AltProjectName,
      IncreasedIncome,
      IncreasedIncomeCohort,
      IncreasedIncomeMath,
      IncreasedIncomePercent,
      IncreasedIncomePoints,
      IncreasedIncomePossible,
      IncreasedIncomeDQ
    )

  # Housing Stability: Length of Time Homeless ------------------------------
  # TH, SH, RRH

  # pe$LengthOfStayMahoning <- pe$HoHsMovedInLeavers %>%
  #   dplyr::right_join(pe_coc_funded %>%
  #                       dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
  #                       unique(),
  #                     by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
  #   dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
  #   dplyr::mutate(DaysInProject = difftime(ExitAdjust, EntryDate, units = "days")) %>%
  #   dplyr::select(ProjectType,
  #                 AltProjectName,
  #                 General_DQ,
  #                 EntryDate,
  #                 EntryAdjust,
  #                 MoveInDateAdjust,
  #                 ExitDate,
  #                 DaysInProject,
  #                 PersonalID,
  #                 UniqueID,
  #                 EnrollmentID,
  #                 HouseholdID)

  # summary_pe$LengthOfStayMahoning <- pe$LengthOfStayMahoning %>%
  #   dplyr::group_by(ProjectType, AltProjectName, General_DQ) %>%
  #   dplyr::summarise(
  #     AverageDays = as.numeric(mean(DaysInProject))
  #   ) %>%
  #   dplyr::ungroup() %>%
  #   dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
  #   dplyr::mutate(
  #     AverageDaysJoin = dplyr::if_else(is.na(AverageDays), 0, AverageDays)) %>%
  #   dplyr::right_join(scoring_rubric %>%
  #                       dplyr::filter(metric == "length_of_stay"),
  #                     by = "ProjectType") %>%
  #   dplyr::group_by(ProjectType) %>%
  #   dplyr::mutate(AverageLoSPossible = max(points)) %>%
  #   dplyr::ungroup() %>%
  #   dplyr::filter(dplyr::if_else(goal_type == "max",
  #                                minimum <= AverageDaysJoin &
  #                                  maximum > AverageDaysJoin,
  #                                minimum < AverageDaysJoin &
  #                                  maximum >= AverageDaysJoin)) %>%
  #   dplyr::mutate(
  #     AverageLoSPoints = dplyr::case_when(
  #       ClientsMovedInLeavers == 0 &
  #         ProjectType != 3 ~ AverageLoSPossible,
  #       TRUE ~ points
  #     ),
  #     AverageLoSMath = dplyr::if_else(
  #       ClientsMovedInLeavers == 0,
  #       "All points granted because this project had 0 leavers who moved into the project's housing",
  #       paste(as.integer(AverageDays), "average days")
  #     ),
  #     AverageLoSDQ = dplyr::case_when(
  #       ProjectType %in% c(2, 8, 13) ~ General_DQ),
  #     AverageLoSPoints = dplyr::case_when(
  #       AverageLoSDQ == 1 ~ 0,
  #       AverageLoSDQ == 0 | is.na(AverageLoSDQ) ~ AverageLoSPoints),
  #     AverageLoSPoints = dplyr::if_else(is.na(AverageLoSPoints), 0, AverageLoSPoints),
  #     AverageLoSCohort = "ClientsMovedInLeavers"
  #   ) %>%
  #   dplyr::select(ProjectType, AltProjectName, AverageLoSMath, AverageLoSCohort,
  #                 AverageLoSPoints, AverageLoSPossible, AverageLoSDQ)

  # Community Need: Res Prior = Streets or ESSH -----------------------------
  # PSH, TH, SH (Street only), RRH

  pe$ResPriorMahoning <- pe$AdultsEntered %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::filter(ProjectType %in% c(2, 3, 13, 8)) %>%
    dplyr::mutate(LHResPriorDQ = dplyr::if_else(General_DQ == 1, 1, 0),
                  MeetsObjective = dplyr::if_else(
                    (ProjectType %in% c(2, 3, 13) &
                       LivingSituation %in% c(101, 116, 118)) |
                      (ProjectType == 8 &
                         LivingSituation == 116),
                    1,
                    0
                  )) %>%
    dplyr::select(dplyr::all_of(vars$we_want), LivingSituation, LHResPriorDQ)

  summary_pe$ResPriorMahoning <- pe$ResPriorMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, LHResPriorDQ) %>%
    dplyr::summarise(LHResPrior = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning,
                      by = c("ProjectType",
                             "AltProjectName")) %>%
    dplyr::mutate(
      LHResPrior = dplyr::if_else(is.na(LHResPrior), 0, LHResPrior),
      LHResPriorPercent = LHResPrior / AdultsEntered,
      LHResPriorPercentJoin = dplyr::if_else(is.na(LHResPriorPercent), 0, LHResPriorPercent)) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "res_prior"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(LHResPriorPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= LHResPriorPercentJoin &
                                   maximum > LHResPriorPercentJoin,
                                 minimum < LHResPriorPercentJoin &
                                   maximum >= LHResPriorPercentJoin)) %>%
    dplyr::mutate(LHResPriorMath = dplyr::if_else(
      AdultsEntered == 0,
      "All points granted because this project has 0 adults who entered the project",
      paste(
        LHResPrior,
        "coming from shelter or streets (unsheltered) /",
        AdultsEntered,
        "adults who entered the project during the reporting period =",
        scales::percent(LHResPriorPercent, accuracy = 1)
      )
    ),
    LHResPriorDQ = dplyr::if_else(is.na(LHResPriorDQ), 0, LHResPriorDQ),
    LHResPriorPoints = dplyr::case_when(
      AdultsEntered == 0 ~ LHResPriorPossible,
      TRUE ~ points),
    LHResPriorPoints = dplyr::case_when(
      LHResPriorDQ == 1 ~ 0,
      LHResPriorDQ == 0 | is.na(LHResPriorDQ) ~ LHResPriorPoints),
    LHResPriorCohort = "AdultsEntered"
    ) %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      LHResPrior,
      LHResPriorCohort,
      LHResPriorMath,
      LHResPriorPercent,
      LHResPriorPoints,
      LHResPriorPossible,
      LHResPriorDQ
    )

  # Community Need: Entries with No Income ----------------------------------
  # PSH, TH, SH, RRH

  pe$EntriesNoIncomeMahoning <- pe$AdultsEntered %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::filter(ProjectType %in% c(2, 3, 13, 8)) %>%
    dplyr::left_join(IncomeBenefits %>%
                       dplyr::select(EnrollmentID,
                                     InformationDate,
                                     IncomeFromAnySource) %>%
                       unique(),
                     by = c("EnrollmentID", "EntryDate" = "InformationDate")) %>%
    dplyr::mutate(
      IncomeFromAnySource = dplyr::if_else(is.na(IncomeFromAnySource),
                                           99L,
                                           IncomeFromAnySource),
      MeetsObjective = dplyr::if_else(IncomeFromAnySource == 0, 1, 0),
      NoIncomeAtEntryDQ = dplyr::if_else(General_DQ == 1 |
                                           Income_DQ == 1, 1, 0)
    ) %>%
    dplyr::select(dplyr::all_of(vars$we_want), IncomeFromAnySource, NoIncomeAtEntryDQ)

  summary_pe$EntriesNoIncomeMahoning <- pe$EntriesNoIncomeMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, NoIncomeAtEntryDQ) %>%
    dplyr::summarise(NoIncomeAtEntry = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      NoIncomeAtEntry = dplyr::if_else(is.na(NoIncomeAtEntry),
                                       0,
                                       NoIncomeAtEntry),
      NoIncomeAtEntryDQ = dplyr::if_else(is.na(NoIncomeAtEntryDQ), 0, NoIncomeAtEntryDQ),
      NoIncomeAtEntryPercent = NoIncomeAtEntry / AdultsEntered,
      NoIncomeAtEntryPercentJoin = dplyr::if_else(is.na(NoIncomeAtEntryPercent), 0, NoIncomeAtEntryPercent)) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "entries_no_income"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(NoIncomeAtEntryPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= NoIncomeAtEntryPercentJoin &
                                   maximum > NoIncomeAtEntryPercentJoin,
                                 minimum < NoIncomeAtEntryPercentJoin &
                                   maximum >= NoIncomeAtEntryPercentJoin)) %>%
    dplyr::mutate(
      NoIncomeAtEntryMath = dplyr::if_else(
        AdultsEntered == 0,
        "All points granted because 0 adults entered this project during the reporting period",
        paste(
          NoIncomeAtEntry,
          "had no income at entry /",
          AdultsEntered,
          "adults who entered the project during the reporting period =",
          scales::percent(NoIncomeAtEntryPercent, accuracy = 1)
        )
      ),
      NoIncomeAtEntryPoints = dplyr::case_when(
        AdultsEntered == 0 ~ NoIncomeAtEntryPossible,
        TRUE ~ points),
      NoIncomeAtEntryPoints = dplyr::case_when(
        NoIncomeAtEntryDQ == 1 ~ 0,
        NoIncomeAtEntryDQ == 0 |
          is.na(NoIncomeAtEntryDQ) ~ NoIncomeAtEntryPoints
      ),
      NoIncomeAtEntryCohort = "AdultsEntered"
    ) %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      NoIncomeAtEntry,
      NoIncomeAtEntryCohort,
      NoIncomeAtEntryMath,
      NoIncomeAtEntryPercent,
      NoIncomeAtEntryPoints,
      NoIncomeAtEntryPossible,
      NoIncomeAtEntryDQ
    )

  # Community Need: Homeless History Index ----------------------------------
  # PSH, TH, SH, RRH

  score_matrix <- as.data.frame(matrix(
    c(0, 1, 1, 2,
      1, 1, 2, 2,
      2, 2, 2, 3,
      3, 3, 4, 4,
      4, 5, 5, 6,
      5, 6, 6, 7),
    nrow = 6, ncol = 4, byrow = TRUE))

  pe$HomelessHistoryIndexMahoning <- pe$AdultsEntered %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      PersonalID,
      UniqueID,
      EnrollmentID,
      HouseholdID,
      AgeAtEntry,
      VeteranStatus,
      EntryDate,
      MoveInDateAdjust,
      ExitDate,
      DateToStreetESSH,
      TimesHomelessPastThreeYears,
      MonthsHomelessPastThreeYears
    ) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::mutate(
      DaysHomelessAtEntry = dplyr::if_else(
        EntryDate >= DateToStreetESSH,
        difftime(EntryDate,
                 DateToStreetESSH,
                 units = "days"),
        NA
      ),
      NumMonthsLevel = dplyr::case_when(
        is.na(MonthsHomelessPastThreeYears) ~ 1,
        MonthsHomelessPastThreeYears == 101 ~ 2,
        MonthsHomelessPastThreeYears %in% c(102, 103, 104) ~ 3,
        MonthsHomelessPastThreeYears %in% c(105, 106, 107, 108) ~ 4,
        MonthsHomelessPastThreeYears %in% c(109, 110, 111) ~ 5,
        MonthsHomelessPastThreeYears %in% c(112, 113) ~ 6
      ),
      TimesLevel = dplyr::case_when(
        is.na(TimesHomelessPastThreeYears) ~ 1,
        TimesHomelessPastThreeYears == 1 ~ 2,
        TimesHomelessPastThreeYears %in% c(2, 3) ~ 3,
        TimesHomelessPastThreeYears == 4 ~ 4
      ),
      HHI = dplyr::if_else(DaysHomelessAtEntry >= 365, 7,
                           score_matrix[cbind(NumMonthsLevel, TimesLevel)]),
      HHI = tidyr::replace_na(HHI, 0) # when null I'm seeing client wasn't even eligible

    ) %>%
    dplyr::select(-NumMonthsLevel, -TimesLevel)

  summary_pe$HomelessHistoryIndexMahoning <- pe$HomelessHistoryIndexMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, General_DQ) %>%
    dplyr::summarise(MedHHI = stats::median(HHI)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "homeless_history_index"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(MedianHHIPossible = max(points),
                  MedHHIJoin = dplyr::if_else(is.na(MedHHI), 0, MedHHI)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= MedHHIJoin &
                                   maximum > MedHHIJoin,
                                 minimum < MedHHIJoin &
                                   maximum >= MedHHIJoin)) %>%
    dplyr::mutate(
      MedianHHIMath = dplyr::if_else(
        AdultsEntered == 0,
        "All points granted since 0 adults entered this project during the reporting period",
        paste("Median Homeless History Index = ", MedHHI)
      ),
      MedianHHIPoints = dplyr::if_else(AdultsEntered == 0, MedianHHIPossible,
                                       points),
      MedianHHIDQ = dplyr::if_else(General_DQ == 1, 1, 0),
      MedianHHIDQ = dplyr::if_else(is.na(MedianHHIDQ), 0, MedianHHIDQ),
      MedianHHIPoints = dplyr::case_when(MedianHHIDQ == 1 ~ 0,
                                         MedianHHIDQ == 0 | is.na(MedianHHIDQ) ~ MedianHHIPoints),
      MedianHHICohort = "AdultsEntered"
    ) %>%
    dplyr::select(ProjectType,
                  AltProjectName,
                  MedHHI,
                  MedianHHIMath,
                  MedianHHICohort,
                  MedianHHIPoints,
                  MedianHHIPossible,
                  MedianHHIDQ)

  # HMIS Data Quality -------------------------------------------------------
  # PSH, TH, SH, RRH

  pe_dq <- dq_for_pe %>%
    dplyr::filter(Type %in% c("Error", "High Priority") &
                    ProjectType %in% c(2, 3, 13, 8)) %>%
    dplyr::inner_join(pe_coc_funded, by = c("ProjectName", "ProjectID", "ProjectType"))


  summary_pe$DQ <- pe_dq %>%
    dplyr::group_by(AltProjectName, ProjectType) %>%
    dplyr::count() %>%
    dplyr::ungroup()

  summary_pe$DQ <- pe_summary_validation_mahoning %>%
    dplyr::select(AltProjectName, ProjectType, ClientsServed) %>%
    dplyr::left_join(summary_pe$DQ, by = c("ProjectType", "AltProjectName"))

  summary_pe$DQ[is.na(summary_pe$DQ)] <- 0

  summary_pe$DQ <- summary_pe$DQ %>%
    dplyr::mutate(DQPercent = n / ClientsServed,
                  DQMath = paste(n,
                                 "errors /",
                                 ClientsServed,
                                 "clients served =",
                                 scales::percent(DQPercent, accuracy = 1)),
                  DQPoints = dplyr::case_when(
                    n == 0 ~ 5,
                    DQPercent > 0 & DQPercent <= .02 ~ 4,
                    DQPercent > .02 & DQPercent <= .05 ~ 3,
                    DQPercent > .05 & DQPercent <= .08 ~ 2,
                    DQPercent > .08 & DQPercent <= .1 ~ 1,
                    DQPercent > .1 ~ 0
                  ),
                  DQPossible = 5,
                  DQCohort = "ClientsServed"
    ) %>%
    dplyr::select(AltProjectName, ProjectType, "DQIssues" = n, DQCohort, DQPercent,
                  DQPoints, DQMath, DQPossible)

  # Community Need: Long Term Homeless Households ---------------------------
  # PSH
  # Decided in Feb meeting that we're going to use Adults Entered for this one

  pe$LongTermHomelessMahoning <- pe$AdultsEntered %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::mutate(
      CurrentHomelessDuration = difftime(EntryDate, DateToStreetESSH,
                                         units = "days"),
      MeetsObjective = dplyr::if_else((
        CurrentHomelessDuration >= 365 &
          !is.na(CurrentHomelessDuration)
      ) |
        (
          TimesHomelessPastThreeYears == 4 &
            MonthsHomelessPastThreeYears %in% c(112, 113) &
            !is.na(TimesHomelessPastThreeYears) &
            !is.na(MonthsHomelessPastThreeYears)
        ),
      1,
      0
      ),
      LTHomelessDQ = dplyr::if_else(ProjectType == 3 & General_DQ == 1, 1, 0)
    ) %>%
    dplyr::select(dplyr::all_of(vars$we_want), DateToStreetESSH,
                  CurrentHomelessDuration, MonthsHomelessPastThreeYears,
                  TimesHomelessPastThreeYears, LTHomelessDQ)

  summary_pe$LongTermHomelessMahoning <- pe$LongTermHomelessMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, LTHomelessDQ) %>%
    dplyr::summarise(LongTermHomeless = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      LongTermHomeless = dplyr::if_else(is.na(LongTermHomeless),
                                        0,
                                        LongTermHomeless),
      LongTermHomelessPercent = dplyr::if_else(AdultsEntered > 0,
                                               LongTermHomeless / AdultsEntered,
                                               NA),
      LongTermHomelessPercentJoin = dplyr::if_else(is.na(LongTermHomelessPercent), 0, LongTermHomelessPercent)) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "long_term_homeless"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(LongTermHomelessPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= LongTermHomelessPercentJoin &
                                   maximum > LongTermHomelessPercentJoin,
                                 minimum < LongTermHomelessPercentJoin &
                                   maximum >= LongTermHomelessPercentJoin)) %>%
    dplyr::mutate(
      LongTermHomelessMath = dplyr::if_else(
        AdultsEntered == 0,
        "All points granted because 0 adults entered this project during the reporting period",
        paste(
          LongTermHomeless,
          "considered to be long-term homeless /",
          AdultsEntered,
          "adults entered the project during the reporting period =",
          scales::percent(LongTermHomelessPercent, accuracy = 1)
        )
      ),
      LongTermHomelessPoints = dplyr::if_else(AdultsEntered == 0 &
                                                ProjectType == 3, LongTermHomelessPossible,
                                              points),
      LongTermHomelessPoints = dplyr::case_when(LTHomelessDQ == 0 |
                                                  is.na(LTHomelessDQ) ~ LongTermHomelessPoints,
                                                LTHomelessDQ == 1 ~ 0),
      LongTermHomelessPoints = dplyr::if_else(is.na(LongTermHomelessPoints), 0,
                                              LongTermHomelessPoints),
      LongTermhomelessCohort = "AdultsEntered"
    ) %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      LongTermHomeless,
      LongTermHomelessPercent,
      LongTermHomelessPoints,
      LongTermHomelessMath,
      LongTermhomelessCohort,
      LongTermHomelessPossible,
      LTHomelessDQ
    )

  # VISPDATs at Entry into PH -----------------------------------------------

  pe$ScoredAtPHEntryMahoning <- pe$HoHsEntered %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = c("AltProjectName")) %>%
    dplyr::left_join(
      dq_for_pe %>%
        dplyr::filter(Issue == "Non-DV HoHs Entering PH or TH without SPDAT") %>%
        dplyr::select("PersonalID", "HouseholdID", "Issue"),
      by = c("PersonalID", "HouseholdID")
    ) %>%
    dplyr::filter(ProjectType != 8) %>%
    dplyr::mutate(
      MeetsObjective = dplyr::case_when(
        !is.na(PersonalID) & is.na(Issue) & ProjectType %in% c(2, 3, 13) ~ 1,
        !is.na(PersonalID) & !is.na(Issue) & ProjectType %in% c(2, 3, 13) ~ 0,
        is.na(PersonalID) & is.na(Issue) & ProjectType %in% c(2, 3, 13) ~ 1),
      ScoredAtEntryDQ = dplyr::case_when(
        ProjectType %in% c(2, 3, 13) & General_DQ == 1 ~ 1,
        ProjectType %in% c(2, 3, 13) & General_DQ == 0 ~ 0)
    ) %>%
    dplyr::select(dplyr::all_of(vars$we_want), ScoredAtEntryDQ)

  summary_pe$ScoredAtPHEntryMahoning <- pe$ScoredAtPHEntryMahoning %>%
    dplyr::group_by(ProjectType, AltProjectName, ScoredAtEntryDQ) %>%
    dplyr::summarise(ScoredAtEntry = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation_mahoning, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      ScoredAtEntry = dplyr::if_else(is.na(ScoredAtEntry),
                                     0,
                                     ScoredAtEntry),
      ScoredAtEntryPercent = dplyr::if_else(HoHsEntered > 0,
                                            ScoredAtEntry / HoHsEntered,
                                            NA),
      ScoredAtEntryPercentJoin = dplyr::if_else(is.na(ScoredAtEntryPercent), 0, ScoredAtEntryPercent)) %>%
    dplyr::right_join(scoring_rubric %>%
                        dplyr::filter(metric == "scored_at_ph_entry"),
                      by = "ProjectType") %>%
    dplyr::group_by(ProjectType) %>%
    dplyr::mutate(ScoredAtEntryPossible = max(points)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= ScoredAtEntryPercentJoin &
                                   maximum > ScoredAtEntryPercentJoin,
                                 minimum < ScoredAtEntryPercentJoin &
                                   maximum >= ScoredAtEntryPercentJoin)) %>%
    dplyr::mutate(
      ScoredAtEntryMath = dplyr::if_else(
        HoHsEntered == 0,
        "All points granted because 0 households entered the project during the reporting period",
        paste(
          ScoredAtEntry,
          "had a VI-SPDAT score at entry /",
          HoHsEntered,
          "heads of household who entered the project during the reporting period =",
          scales::percent(ScoredAtEntryPercent, accuracy = 1)
        )
      ),
      ScoredAtEntryPoints = dplyr::case_when(
        HoHsEntered == 0 ~ ScoredAtEntryPossible,
        HoHsEntered > 0 ~ points),
      ScoredAtEntryPoints = dplyr::case_when(
        ScoredAtEntryDQ == 0 ~ ScoredAtEntryPoints,
        ScoredAtEntryDQ == 1 ~ 0,
        is.na(ScoredAtEntryDQ) ~ ScoredAtEntryPoints),
      ScoredAtEntryPoints = dplyr::if_else(is.na(ScoredAtEntryPoints),
                                           0,
                                           ScoredAtEntryPoints),
      ScoredAtEntryCohort = "HoHsEntered"
    ) %>%
    dplyr::select(
      ProjectType,
      AltProjectName,
      ScoredAtEntry,
      ScoredAtEntryMath,
      ScoredAtEntryPercent,
      ScoredAtEntryPoints,
      ScoredAtEntryCohort,
      ScoredAtEntryPossible,
      ScoredAtEntryDQ
    )

  # Final Scoring -----------------------------------------------------------

  # all the alt-projects & score details & totals
  pe_summary <- purrr::reduce(summary_pe, ~dplyr::left_join(.x, .y, by = c("ProjectType", "AltProjectName")))

  pe_summary_final_scoring_mahoning <-
    pe_coc_funded[c("ProjectType", "AltProjectName")] %>%
    unique() %>%
    dplyr::left_join(pe_summary, by = c("ProjectType", "AltProjectName")) |>
    dplyr::left_join(summary_pe_coc_scoring, by = c("ProjectType", "AltProjectName"))


  pe_final_scores <- pe_summary_final_scoring_mahoning

  pe_final_scores$HousingFirstScore[is.na(pe_final_scores$HousingFirstScore)] <- 0
  pe_final_scores$ChronicPrioritizationScore[is.na(pe_final_scores$ChronicPrioritizationScore)] <- 0
  pe_final_scores$PrioritizationWorkgroupScore[is.na(pe_final_scores$PrioritizationWorkgroupScore)] <- 0
  # pe_final_scores$AverageLoSPoints[is.na(pe_final_scores$AverageLoSPoints)] <- 0
  pe_final_scores$LongTermHomelessPoints[is.na(pe_final_scores$LongTermHomelessPoints)] <- 0

  pe_final_scores <- pe_final_scores %>%
    dplyr::mutate(
      TotalScore = DQPoints +
        NoIncomeAtEntryPoints +
        ExitsToPHPoints +
        ScoredAtEntryPoints +
        # MedianHHIPoints +
        IncreasedIncomePoints +
        # AverageLoSPoints +
        LongTermHomelessPoints +
        BenefitsAtExitPoints +
        OwnHousingPoints +
        LHResPriorPoints +
        HousingFirstScore +
        ChronicPrioritizationScore +
        PrioritizationWorkgroupScore
    ) %>%
    dplyr::select(ProjectType,
                  AltProjectName,
                  dplyr::ends_with("Points"),
                  dplyr::ends_with("Score"),
                  dplyr::ends_with("Scoring"),
                  TotalScore)

  # adding in Organization Name for publishing the final ranking
  # Org Names for the combined projects have to be done manually

  Organization <- Organization # get Organization?
  project_and_alt_project <- pe_coc_funded %>%
    dplyr::left_join(Project[c("ProjectID", "OrganizationID")], by = "ProjectID") %>%
    dplyr::left_join(Organization[c("OrganizationID", "OrganizationName")],
                     by = "OrganizationID")

  final_scores <- pe_final_scores %>%
    dplyr::select(AltProjectName, TotalScore) %>%
    dplyr::left_join(project_and_alt_project, by = c("AltProjectName" = "AltProjectName")) %>%
    dplyr::select(OrganizationName, AltProjectName, TotalScore) %>%
    dplyr::arrange(dplyr::desc(TotalScore))


  # commenting all this out since we don't want to overwrite these files after
  # the deadline

  zero_divisors <- pe_summary_validation_mahoning %>%
    dplyr::filter(ClientsServed == 0 |
                    HoHsEntered == 0 |
                    HoHsServed == 0 |
                    HoHsServedLeavers == 0 |
                    # AdultsMovedIn == 0 |
                    AdultsEntered == 0 |
                    ClientsMovedInLeavers == 0 |
                    AdultsMovedInLeavers == 0 |
                    HoHsMovedInLeavers == 0) %>%
    dplyr::select(-HoHDeaths)

  # TODO These need to be send somewhere rather than saved
  # readr::write_csv(zero_divisors, fs::path(dirs$random, "zero_divisors_mahoning.csv"))
  #
  # readr::write_csv(final_scores %>%
  #                    dplyr::select(OrganizationName,
  #                                  AltProjectName,
  #                                  TotalScore), fs::path(dirs$random, "pe_final_consolidated_projects_mahoning.csv"))
  #
  # readr::write_csv(pe_final_scores, fs::path(dirs$random, "pe_final_all_mahoning.csv"))

  exported_pe <- pe[c("ScoredAtPHEntryMahoning", "LongTermHomelessMahoning", "HomelessHistoryIndexMahoning", "IncreaseIncomeMahoning", "OwnHousingMahoning", "ResPriorMahoning", "BenefitsAtExitMahoning", "ExitsToPHMahoning", "EntriesNoIncomeMahoning")] |>
    {\(x) {rlang::set_names(x, paste0("pe_", snakecase::to_snake_case(names(x))))}}()

  # saving old data to "current" image so it all carries to the apps
  rlang::exec(app_env$gather_deps, pe_summary_final_scoring_mahoning = pe_summary_final_scoring_mahoning, !!!exported_pe)
}




