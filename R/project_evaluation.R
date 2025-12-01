
#' @include 06_Project_Evaluation_utils.R
project_evaluation <- function(
  Project,
  Funder,
  Enrollment_extra_Client_Exit_HH_CL_AaE,
  rm_dates
) {

  co_clients_served <- HMISdata::load_hmis_parquet("co_clients_served.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_adults_served <- HMISdata::load_hmis_parquet("co_adults_served.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_hohs_served <- HMISdata::load_hmis_parquet("co_hohs_served.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_hohs_entered <- HMISdata::load_hmis_parquet("co_hohs_entered.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_clients_moved_in_leavers <- HMISdata::load_hmis_parquet("co_clients_moved_in_leavers.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_hohs_moved_in_leavers <- HMISdata::load_hmis_parquet("co_hohs_moved_in_leavers.parquet", bucket = "hud.csv-daily", "hmis_output")
  co_adults_moved_in_leavers <- HMISdata::load_hmis_parquet("co_adults_moved_in_leavers.parquet", bucket = "hud.csv-daily", "hmis_output")

  # read scoring rubric from google sheets
  googlesheets4::gs4_auth(path = "inst/vault/rminor@rminor-333915.iam.gserviceaccount.com.json")
  scoring_rubric <- googlesheets4::read_sheet("1lLsNI8A2E-dDE8O2EHmCP9stSImxZkYJTGx-Oxs1W74",
                                              sheet = "Sheet1",
                                              col_types = c("metric" = "c", "goal_type" = "c", "minimum" = "n", "maximum" = "n",
                                                            "points" = "n") |> paste0(collapse = ""))

  merged_projects <- setNames(
    list(
      list(c("GLCAP", "Homenet", "PSH")),
      list(c("Athens - Integrated Services - YHDP RRH"), c("Vinton - Integrated Services - YHDP RRH"), c("Meigs - Integrated Services - YHDP RRH"), c("Jackson - Integrated Services - YHDP RRH"), c("Gallia - Integrated Services - YHDP RRH")),
      list(c("Ashland - One Eighty - PSH"), c("Wayne - One Eighty - PSH")),
      list(c("Coalition for Housing - Region 9"), c("Partners of Central Ohio - Region 9"), c("Coshocton - Knohoco Ashland CAC - Region 9 RRH")),
      list(c("Athens - Integrated Services - Charles Place - PSH"), c("Athens - Integrated Services - Graham Drive Family Housing - PSH")),
      list(c("Mental Health Recovery Board of Preble County - Prestwick Square - PSH"), c("Mental Health Recovery Board of Preble County - Prestwick Square II - PSH"))
    ),
    c(
      "GLCAP - PSH - Combined",
      "Integrated Services - YDHP - RRH",
      "One Eighty Plus Care - PSH - Combined",
      "Licking Region 9 - RRH - Combined",
      "Athens - Integrated Services - Charles/Graham Combined",
      "Preble MHRB \u2013 Prestwick Square PSH \u2013 Combined"
    )
  )


  merged_projects <- purrr::map(merged_projects, ~{
    reg <- purrr::map(.x, ~UU::regex_op(.x, "&"))
    idx <- purrr::map(reg, ~{
      matches <- stringr::str_which(Project$ProjectName, .x)
      if (length(matches) == 0) integer(0) else matches
    })
    idx <- unlist(idx)

    list(ProjectName = Project$ProjectName[idx],
         ProjectID = Project$ProjectID[idx])
  })

  .merged <- rlang::set_names(purrr::map(merged_projects, "ProjectID") |> purrr::flatten_chr(), purrr::map(merged_projects, "ProjectName") |> purrr::flatten_chr())

  # Add additional consolidated projects to list
  # Get the project names for those IDs
  new_ids <- c("1306", "1307", "1321")
  new_names <- Project$ProjectName[Project$ProjectID %in% new_ids]

  # Add to .merged
  new_entries <- setNames(new_ids, new_names)
  .merged <- c(.merged, new_entries)

  # consolidated projects
  pe_coc_funded <- Funder %>%
    dplyr::filter(Funder %in% c(1:7, 43, 44) &
                    (ProjectID %in% .merged |
                       (
                         StartDate <= rm_dates$hc$project_eval_end &
                           (is.na(EndDate) |
                              EndDate >= rm_dates$hc$project_eval_end)
                       ))) %>%
    dplyr::select(ProjectID, Funder, StartDate, EndDate) %>%
    dplyr::left_join(Project[c("ProjectID",
                               "ProjectName",
                               "ProjectType",
                               "HMISParticipationType",
                               "ProjectRegion")], by = "ProjectID") %>%
    dplyr::filter(HMISParticipationType == 1 &
                    ProjectRegion != 0) %>%
    dplyr::mutate(
      AltProjectName = merge_projects(ProjectName, merged_projects),
      AltProjectID = merge_projects(ProjectID, merged_projects),
      ProjectType = as.character(ProjectType)
    ) |>
    dplyr::select(ProjectType,
                  ProjectName,
                  ProjectID,
                  AltProjectName,
                  AltProjectID)

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

  # clients served during date ranged

  # no dupes w/in a project
  pe <- list()
  pe$ClientsServed <- peval_filter_select(co_clients_served,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE, vars = vars$prep,  served = TRUE)
  
  # Adults served and leaved (for income measures)
  co_clients_served_adults <- co_clients_served |>
    dplyr::mutate(AgeAtCompetitionStart = lubridate::interval(DOB, rm_dates$hc$project_eval_start) / lubridate::years(1)) |> 
    dplyr::filter(AgeAtCompetitionStart >= 18)
  
  pe$AdultsServed <- peval_filter_select(co_clients_served_adults,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE, vars = vars$prep,  served = TRUE)

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

  pe$AdultsEntered <- peval_filter_select(co_adults_served, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    vars = vars$prep, distinct = FALSE) |>
    dplyr::group_by(HouseholdID) %>%
    dplyr::mutate(HHEntryDate = min(EntryDate)) %>%
    dplyr::ungroup() |>
    HMIS::entered_between(rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end) |>
    dplyr::filter(EntryDate == HHEntryDate) |>
    dplyr::select(-HHEntryDate)


  # counts each client's entry

  ## for vispdat measure

  pe$HoHsEntered <- peval_filter_select(co_hohs_entered, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    vars = vars$prep, entered = TRUE, distinct = FALSE)

  # for ncb logic
  # Adults who moved in and exited during date range

  pe$AdultsMovedInLeavers <- peval_filter_select(co_adults_moved_in_leavers,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE, 
    vars = vars$prep, stayed = TRUE, exited = TRUE)


  # health insurance
  # Clients who moved in and exited during date range

  pe$ClientsMovedInLeavers <- peval_filter_select(co_clients_moved_in_leavers,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    vars = vars$prep, stayed = TRUE, exited = TRUE)

  # exits to PH, but needs an added filter of only mover-inners
  # Heads of Household who were served during date range

  pe$HoHsServed <- peval_filter_select(co_hohs_served, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    vars = vars$prep, served = TRUE)

  pe$HoHsServedLeavers <- peval_filter_select(co_hohs_served, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    vars = vars$prep, served = TRUE, exited = TRUE)

  # own housing and LoS
  # Heads of Household who moved in and exited during date range

  pe$HoHsMovedInLeavers <- peval_filter_select(co_hohs_moved_in_leavers, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    vars = vars$prep, stayed = TRUE, exited = TRUE)


  # Create Validation Summary -----------------------------------------------

  # summary_pe_[cohort] - takes client-level pe_[cohort], calculates # of total
  # clients in the cohort at the alt-project level



  pe_summary_validation <- rlang::set_names(pe, paste0("summary_", names(pe))) |>
    purrr::imap(peval_summary, pe_coc_funded = pe_coc_funded) |>
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

  dq_for_pe <- HMISdata::load_hmis_parquet("dq_for_pe.parquet", bucket = "shiny-data-cohhio", folder = "RME")

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

  data_quality_flags <- pe_summary_validation %>%
    dplyr::left_join(dq_flags_staging, by = "AltProjectName") %>%
    dplyr::mutate(General_DQ = dplyr::if_else(GeneralFlagTotal/ClientsServed >= .02, 1, 0),
                  Benefits_DQ = dplyr::if_else(BenefitsFlagTotal/AdultsEntered >= .02, 1, 0),
                  Income_DQ = dplyr::if_else(IncomeFlagTotal/AdultsEntered >= .02, 1, 0),
                  LoTH_DQ = dplyr::if_else(LoTHFlagTotal/HoHsServed >= .02, 1, 0))

  data_quality_flags[is.na(data_quality_flags)] <- 0

  # writing out a file to help notify flagged projects toward end of process

  # TODO Automation email drafts to the right set of users (need to filter COHHIO_admin_user_ids)
  # Retrieve AgencyID, get all attached UserIDs, Send email to those Users.

  User_extras <- HMISdata::load_looker_data(filename = "User", col_types = HMISdata::look_specs$User)

  pe_users_info <- data_quality_flags %>%
    dplyr::filter(GeneralFlagTotal > 0 |
                    BenefitsFlagTotal > 0 |
                    IncomeFlagTotal > 0 |
                    LoTHFlagTotal > 0) %>%
    dplyr::left_join(pe_coc_funded %>%
                       dplyr::distinct(ProjectID, AltProjectID), by = "AltProjectID") %>%
    dplyr::mutate(ProjectID = dplyr::if_else(is.na(ProjectID), AltProjectID, ProjectID)) %>%
    dplyr::left_join(User_extras |>
                       dplyr::mutate(ProjectID = as.character(ProjectID)) |>
                       dplyr::filter(!is.na(ProjectID) & Deleted == "No") |>
                       dplyr::select(UserID, ProjectID), by = "ProjectID")

  # this file ^^ is used by Reports/CoC_Competition/Notify_DQ.Rmd to produce
  # emails to all users attached to any of the providers with DQ flags.

  # displays flags thrown at the alt-project level

  data_quality_flags <- data_quality_flags %>%
    dplyr::select(AltProjectName, dplyr::ends_with("DQ"))

  
  summary_pe <- list()
  # Housing Stability: Exits to PH ------------------------------------------

  # pe_[measure] - client-level dataset of all clients counted in the measure
  # along with whether each one meets the objective
  # summary_pe_[measure] - uses pe_[measure] to smush to alt-project level and
  # adds a score

  destinations <- HMISprep::destinations

  #### Housing Stability

  # Measure 1
  # % heads of household who were served in the date range and remained in project as of end of reporting period or exited to PH during the reporting period
  # PSH (includes stayers), TH, SH, RRH
  pe$ExitsToPH <- pe$HoHsServed %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") |>
    {\(x) {dplyr::filter(x, (ProjectType %in% c(2, 8, 13) &
                               HMIS::exited_between(x, rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end, lgl = TRUE)) |
                           ProjectType == 3)}}() |> # filtering out non-PSH stayers
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

  summary_pe$ExitsToPH <- pe$ExitsToPH %>%
    dplyr::group_by(ProjectType, AltProjectName, ExitsToPHDQ) %>%
    dplyr::summarise(ExitsToPH = sum(MeetsObjective), .groups = "drop") %>%
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) %>%
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
    dplyr::mutate(AltProjectType = dplyr::if_else(stringr::str_detect(AltProjectName, "Integrated Services - YDHP - RRH"), "113",
                                                  dplyr::if_else(AltProjectName == "Vinton - Sojourners Care Network - YHDP Crisis TH", "102", ProjectType))
    ) |>
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "exits_to_ph")) %>%
    dplyr::mutate(ExitsToPHPossible = max(points)) %>%
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
          scales::percent(ExitsToPHPercent, accuracy = 0.1)
        ),
      ProjectType != 3 & HoHsServedLeavers != 0 ~
        paste(
          ExitsToPH,
          "exits to permanent housing /",
          HoHsServedLeavers,
          "heads of household leavers =",
          scales::percent(ExitsToPHPercent, accuracy = 0.1)
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
  
  # Measure 2 NEW
  # % heads of household who returned to homelessness at program exit
  
  `%nin%` <- purrr::negate(`%in%`)

  pe$ReturnToHomelessness <- pe$HoHsServed %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") |>
    {\(x) {dplyr::filter(x, (HMIS::exited_between(x, rm_dates$hc$project_eval_start, rm_dates$hc$project_eval_end, lgl = TRUE)))}}() |>
    dplyr::mutate(
      DestinationGroup = dplyr::case_when(
        is.na(Destination) | ExitAdjust > rm_dates$hc$project_eval_end ~
          "Still in Program at Report End Date",
        Destination %in% c(101, 116, 118) ~ "Homeless",
        Destination %nin% c(24, 101, 116, 118) ~ "Not Homeless",
        Destination == 24 ~ "Deceased (not counted)",
        Destination %in% destinations$other ~ "Other"
      ),
      ReturnToHomelessnessDQ = dplyr::case_when(
        General_DQ == 1 ~ 1,
        TRUE ~ 0
      ),
      MeetsObjective =
        dplyr::case_when(
          DestinationGroup == "Not Homeless" ~ 1,
          TRUE ~ 0
        )
    ) |> dplyr::select(dplyr::all_of(vars$we_want), ReturnToHomelessnessDQ, Destination, DestinationGroup)

    summary_pe$ReturnToHomelessness <- pe$ReturnToHomelessness %>%
      dplyr::group_by(ProjectType, AltProjectName, ReturnToHomelessnessDQ) %>%
      dplyr::summarise(NotHomeless = sum(MeetsObjective), .groups = "drop") %>%
      dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) %>%
      dplyr::mutate(
        HoHsServedLeavers = HoHsServedLeavers - HoHDeaths,
        ReturnToHomelessnessCohort = HoHsServedLeavers,
        NotHomeless = dplyr::if_else(is.na(NotHomeless), 0, NotHomeless),
        ReturnToHomelessness = HoHsServedLeavers - NotHomeless,
        ReturnToHomelessnessPercent = 1 - (NotHomeless / HoHsServedLeavers),
        ReturnToHomelessnessPercentJoin = dplyr::if_else(is.na(ReturnToHomelessnessPercent), 0, ReturnToHomelessnessPercent)) |>
        dplyr::cross_join(
          scoring_rubric %>%
            dplyr::filter(metric == "return_to_homelessness")
        ) |> 
      dplyr::mutate(ReturnToHomelessnessPossible = max(points)) %>%
      dplyr::filter(dplyr::if_else(goal_type == "min",
        minimum <= ReturnToHomelessnessPercentJoin &
        maximum > ReturnToHomelessnessPercentJoin,
        minimum < ReturnToHomelessnessPercentJoin &
        maximum >= ReturnToHomelessnessPercentJoin)) %>%
      dplyr::mutate(ReturnToHomelessnessMath = dplyr::case_when(
            is.na(HoHsServedLeavers) | HoHsServedLeavers == 0 ~ "No exits during the period.",
            TRUE ~ paste(
              HoHsServedLeavers - NotHomeless,
              "exits to homeless situation /",
              HoHsServedLeavers,
              "heads of household =",
              scales::percent(ReturnToHomelessnessPercent, accuracy = 0.1)
            )
      ),
      ReturnToHomelessnessPoints = dplyr::if_else(HoHsServedLeavers == 0, ReturnToHomelessnessPossible, points),
      ReturnToHomelessnessPoints = dplyr::if_else(
        ReturnToHomelessnessDQ == 0 | is.na(ReturnToHomelessnessDQ),
        ReturnToHomelessnessPoints,
        0
      )
      ) %>%
      dplyr::select(
        ProjectType,
        AltProjectName,
        ReturnToHomelessness,
        ReturnToHomelessnessMath,
        ReturnToHomelessnessPercent,
        ReturnToHomelessnessPoints,
        ReturnToHomelessnessPossible,
        ReturnToHomelessnessDQ,
        ReturnToHomelessnessCohort
      )
  
  #### Accessing Mainstream Resources: Benefits -----------------------------------
  # PSH, TH, SH, RRH

  # Measure 3
  # % adult participants who entered the project during the date range who had 1+ source of non-cash benefits or health insurance at exit

  IncomeBenefits <- HMISdata::load_hmis_parquet("IncomeBenefits.parquet")

  pe$BenefitsAtExit <- pe$AdultsMovedInLeavers %>%
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

  summary_pe$BenefitsAtExit <- pe$BenefitsAtExit %>%
    dplyr::group_by(ProjectType, AltProjectName, BenefitsAtExitDQ) %>%
    dplyr::summarise(BenefitsAtExit = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      BenefitsAtExit = dplyr::if_else(is.na(BenefitsAtExit), 0, BenefitsAtExit),
      BenefitsAtExitPercent = BenefitsAtExit / AdultsMovedInLeavers,
      BenefitsAtExitPercentJoin = dplyr::if_else(is.na(BenefitsAtExitPercent), 0, BenefitsAtExitPercent)) %>%
    dplyr::mutate(AltProjectType = dplyr::if_else(stringr::str_detect(AltProjectName, "Integrated Services - YDHP - RRH"), "113",
                                                  dplyr::if_else(AltProjectName == "Vinton - Sojourners Care Network - YHDP Crisis TH", "102", ProjectType))
    ) |>
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "benefits_at_exit")) %>%
    dplyr::mutate(BenefitsAtExitPossible = max(points)) %>%
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
        scales::percent(BenefitsAtExitPercent, accuracy = 0.1)
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

  # Measure 4
  # % adult participants who gained or increased their total income (from all sources) as of the end of the reporting period or at program exit
  # Accessing Mainstream Resources: Increase Total Income -------------------
  # PSH, TH, RRH

  income_staging2 <-  pe$AdultsServed %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::filter(ProjectType %in% c(2, 3, 13)) |>
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

  pe$IncreaseIncome <- income_staging %>%
    tidyr::pivot_wider(names_from = DataCollectionStage,
                       values_from = TotalMonthlyIncome) %>%
    dplyr::left_join(pe$AdultsServed, by = c("PersonalID", "EnrollmentID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::mutate(
      MostRecentIncome = dplyr::case_when(
        !is.na(Exit) ~ Exit,
        !is.na(Update) ~ Update,
        !is.na(Annual) ~ Annual
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
      tidyselect::all_of(vars$we_want),
      IncreasedIncomeDQ,
      IncomeAtEntry,
      IncomeMostRecent
    )

  rm(list = ls(pattern = "income_staging"))

  summary_pe$IncreaseIncome <- pe$IncreaseIncome |>
    dplyr::group_by(ProjectType, AltProjectName, IncreasedIncomeDQ) |>
    dplyr::summarise(IncreasedIncome = sum(MeetsObjective)) |>
    dplyr::ungroup() |>
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) |>
    dplyr::mutate(
      IncreasedIncome = dplyr::if_else(is.na(IncreasedIncome), 0, IncreasedIncome),
      IncreasedIncomeDQ = dplyr::if_else(is.na(IncreasedIncomeDQ), 0, IncreasedIncomeDQ),
      IncreasedIncomePercent = IncreasedIncome / AdultsServed,
      IncreasedIncomePercentJoin = dplyr::if_else(is.na(IncreasedIncomePercent), 0, IncreasedIncomePercent)
    ) |> 
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "increase_income")) %>%
    dplyr::mutate(IncreasedIncomePossible = max(points)) %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= IncreasedIncomePercentJoin &
                                   maximum > IncreasedIncomePercentJoin,
                                 minimum < IncreasedIncomePercentJoin &
                                   maximum >= IncreasedIncomePercentJoin)) %>%
    dplyr::mutate(
      IncreasedIncomeMath = dplyr::if_else(
        AdultsServed != 0,
        paste(
          IncreasedIncome,
          "increased income during their stay /",
          AdultsServed,
          "adults who moved into the project's housing =",
          scales::percent(IncreasedIncomePercent, accuracy = 0.1)
        ),
        "All points granted because 0 adults moved into the project's housing"
      ),
      IncreasedIncomePoints = dplyr::case_when(
        AdultsServed == 0 ~ IncreasedIncomePossible,
        TRUE ~ points),
      IncreasedIncomePoints = dplyr::case_when(
        IncreasedIncomeDQ == 1 ~ 0,
        AdultsServed != 0 &
          (IncreasedIncomeDQ == 0 | is.na(IncreasedIncomeDQ)) ~ IncreasedIncomePoints
      ),
      IncreasedIncomeCohort = "AdultsServed"
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

  # Measure 5
  # % adult participants who increased earned income at program exit. 
  # NEW

  income_staging3 <-  pe$AdultsMovedInLeavers %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::filter(ProjectType %in% c(2, 3, 13)) |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) %>%
    dplyr::select(PersonalID,
                  EnrollmentID,
                  EntryDate,
                  ExitDate,
                  EarnedAmount,
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

  income_staging_fixed <- income_staging3 %>%
    dplyr::filter(DataCollectionStage == "Entry")

  income_staging_variable <- income_staging3 %>%
    dplyr::filter(DataCollectionStage %in% c("Update", "Annual", "Exit")) %>%
    dplyr::group_by(EnrollmentID) %>%
    dplyr::mutate(MaxUpdate = max(lubridate::ymd_hms(DateCreated))) %>%
    dplyr::filter(MaxUpdate == DateCreated) %>%
    dplyr::select(-MaxUpdate) %>%
    dplyr::distinct() %>%
    dplyr::ungroup()

  income_staging <- rbind(income_staging_fixed, income_staging_variable) %>%
    dplyr::select(PersonalID, EnrollmentID, EarnedAmount, DataCollectionStage) %>%
    unique()

  pe$IncreaseEarnedIncome <- income_staging %>%
    tidyr::pivot_wider(names_from = DataCollectionStage,
                       values_from = EarnedAmount) %>%
    dplyr::left_join(pe$AdultsMovedInLeavers, by = c("PersonalID", "EnrollmentID")) %>%
    dplyr::left_join(data_quality_flags, by = "AltProjectName") %>%
    dplyr::mutate(
      MostRecentEarnedIncome = dplyr::case_when(
        !is.na(Exit) ~ Exit,
        !is.na(Update) ~ Update,
        !is.na(Annual) ~ Annual
      ),
      EarnedIncomeAtEntry = dplyr::if_else(is.na(Entry), 0, Entry),
      EarnedIncomeMostRecent = dplyr::if_else(is.na(MostRecentEarnedIncome),
                                        EarnedIncomeAtEntry,
                                        MostRecentEarnedIncome),
      MeetsObjective = dplyr::case_when(
        EarnedIncomeMostRecent > EarnedIncomeAtEntry ~ 1,
        EarnedIncomeMostRecent <= EarnedIncomeAtEntry ~ 0),
      IncreasedEarnedIncomeDQ = dplyr::if_else(General_DQ == 1 |
                                           Income_DQ == 1, 1, 0),
      PersonalID = as.character(PersonalID)
    ) %>%
    dplyr::select(
      tidyselect::all_of(vars$we_want),
      IncreasedEarnedIncomeDQ,
      EarnedIncomeAtEntry,
      EarnedIncomeMostRecent
    )

  rm(list = ls(pattern = "income_staging"))

  summary_pe$IncreaseEarnedIncome <- pe$IncreaseEarnedIncome |>
    dplyr::group_by(ProjectType, AltProjectName, IncreasedEarnedIncomeDQ) |>
    dplyr::summarise(IncreasedEarnedIncome = sum(MeetsObjective)) |>
    dplyr::ungroup() |>
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) |>
    dplyr::mutate(
      IncreasedEarnedIncome = dplyr::if_else(is.na(IncreasedEarnedIncome), 0, IncreasedEarnedIncome),
      IncreasedEarnedIncomeDQ = dplyr::if_else(is.na(IncreasedEarnedIncomeDQ), 0, IncreasedEarnedIncomeDQ),
      IncreasedEarnedIncomePercent = IncreasedEarnedIncome / AdultsMovedInLeavers,
      IncreasedEarnedIncomePercentJoin = dplyr::if_else(is.na(IncreasedEarnedIncomePercent), 0, IncreasedEarnedIncomePercent)
    ) %>%
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "increase_earned_income")) %>%
    dplyr::mutate(IncreasedEarnedIncomePossible = max(points)) %>%
    dplyr::filter(dplyr::if_else(goal_type == "max",
                                 minimum <= IncreasedEarnedIncomePercentJoin &
                                   maximum > IncreasedEarnedIncomePercentJoin,
                                 minimum < IncreasedEarnedIncomePercentJoin &
                                   maximum >= IncreasedEarnedIncomePercentJoin)) %>%
    dplyr::mutate(
      IncreasedEarnedIncomeMath = dplyr::if_else(
        AdultsMovedInLeavers != 0,
        paste(
          IncreasedEarnedIncome,
          "increased earned income during their stay /",
          AdultsMovedInLeavers,
          "adults who moved into the project's housing =",
          scales::percent(IncreasedEarnedIncomePercent, accuracy = 0.1)
        ),
        "All points granted because 0 adults moved into the project's housing"
      ),
      IncreasedEarnedIncomePoints = dplyr::case_when(
        AdultsMovedInLeavers == 0 ~ IncreasedEarnedIncomePossible,
        TRUE ~ points),
      IncreasedEarnedIncomePoints = dplyr::case_when(
        IncreasedEarnedIncomeDQ == 1 ~ 0,
        AdultsMovedInLeavers != 0 &
          (IncreasedEarnedIncomeDQ == 0 | is.na(IncreasedEarnedIncomeDQ)) ~ IncreasedEarnedIncomePoints
      ),
      IncreasedEarnedIncomeCohort = "AdultsMovedInLeavers"
    ) |>
    dplyr::select(
      ProjectType,
      AltProjectName,
      IncreasedEarnedIncome,
      IncreasedEarnedIncomeCohort,
      IncreasedEarnedIncomeMath,
      IncreasedEarnedIncomePercent,
      IncreasedEarnedIncomePoints,
      IncreasedEarnedIncomePossible,
      IncreasedEarnedIncomeDQ
    )
  

  # Meaure 6
  # % adult who entered project during the date range and came from streets/emergency shelter only
  # Community Need: Res Prior = Streets or ESSH -----------------------------
  # PSH, TH, SH (Street only), RRH

  pe$ResPrior <- pe$AdultsEntered %>%
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

  summary_pe$ResPrior <- pe$ResPrior %>%
    dplyr::group_by(ProjectType, AltProjectName, LHResPriorDQ) %>%
    dplyr::summarise(LHResPrior = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation,
                      by = c("ProjectType",
                             "AltProjectName")) %>%
    dplyr::mutate(
      LHResPrior = dplyr::if_else(is.na(LHResPrior), 0, LHResPrior),
      LHResPriorPercent = LHResPrior / AdultsEntered,
      LHResPriorPercentJoin = dplyr::if_else(is.na(LHResPriorPercent), 0, LHResPriorPercent)) %>%
    dplyr::mutate(AltProjectType = dplyr::if_else(stringr::str_detect(AltProjectName, "Integrated Services - YDHP - RRH"), "113",
                                                  dplyr::if_else(AltProjectName == "Vinton - Sojourners Care Network - YHDP Crisis TH", "102", ProjectType))
    ) |>
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "res_prior")) %>%
    dplyr::mutate(LHResPriorPossible = max(points)) %>%
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
        scales::percent(LHResPriorPercent, accuracy = 0.1)
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

  # Measure 7
  # %  adult who entered project during the date range with no income
  # Community Need: Entries with No Income ----------------------------------
  # PSH, TH, SH, RRH

  pe$EntriesNoIncome <- pe$AdultsEntered %>%
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

  summary_pe$EntriesNoIncome <- pe$EntriesNoIncome %>%
    dplyr::group_by(ProjectType, AltProjectName, NoIncomeAtEntryDQ) %>%
    dplyr::summarise(NoIncomeAtEntry = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      NoIncomeAtEntry = dplyr::if_else(is.na(NoIncomeAtEntry),
                                       0,
                                       NoIncomeAtEntry),
      NoIncomeAtEntryDQ = dplyr::if_else(is.na(NoIncomeAtEntryDQ), 0, NoIncomeAtEntryDQ),
      NoIncomeAtEntryPercent = NoIncomeAtEntry / AdultsEntered,
      NoIncomeAtEntryPercentJoin = dplyr::if_else(is.na(NoIncomeAtEntryPercent), 0, NoIncomeAtEntryPercent)) %>%
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "entries_no_income")) %>%
    dplyr::mutate(NoIncomeAtEntryPossible = max(points)) %>%
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
          scales::percent(NoIncomeAtEntryPercent, accuracy = 0.1)
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

  # Measure 8
  # Median Homeless History Index score for adults who entered project during the reporting period
  # (Homeless History Index is based on number of past homeless episodes and total duration of homelessness)
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

  pe$HomelessHistoryIndex <- pe$AdultsEntered %>%
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

  summary_pe$HomelessHistoryIndex <- pe$HomelessHistoryIndex %>%
    dplyr::group_by(ProjectType, AltProjectName, General_DQ) %>%
    dplyr::summarise(MedHHI = stats::median(HHI)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(AltProjectType = dplyr::if_else(stringr::str_detect(AltProjectName, "Integrated Services - YDHP - RRH"), "113",
                                                  dplyr::if_else(AltProjectName == "Vinton - Sojourners Care Network - YHDP Crisis TH", "102", ProjectType))
    ) |>
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "homeless_history_index")) %>%
    dplyr::mutate(MedianHHIPossible = max(points),
                  MedHHIJoin = dplyr::if_else(is.na(MedHHI), 0, MedHHI)) %>%
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

  summary_pe$DQ <- pe_summary_validation %>%
    dplyr::select(AltProjectName, ProjectType, ClientsServed) %>%
    dplyr::left_join(summary_pe$DQ, by = c("ProjectType", "AltProjectName"))

  summary_pe$DQ[is.na(summary_pe$DQ)] <- 0

  summary_pe$DQ <- summary_pe$DQ %>%
    dplyr::mutate(DQPercent = n / ClientsServed,
                  DQMath = paste(n,
                                 "errors /",
                                 ClientsServed,
                                 "clients served =",
                                 scales::percent(DQPercent, accuracy = 0.1)),
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

  # Measure 9
  # % heads of household who entered the project during the date range and had an assessment (VI-SPDAT or HARP) recorded in HMIS 
  # (excludes clients for whom a current episode of DV was reported or who reported as currently fleeing)

  # VISPDATs at Entry into PH -----------------------------------------------

  pe$ScoredAtPHEntry <- pe$HoHsEntered %>%
    dplyr::right_join(pe_coc_funded %>%
                        dplyr::select(ProjectType, AltProjectID, AltProjectName) %>%
                        unique(),
                      by = c("AltProjectName", "ProjectType", "AltProjectID")) %>%
    dplyr::left_join(data_quality_flags, by = c("AltProjectName")) %>%
    dplyr::left_join(
      dq_for_pe %>%
        dplyr::filter(Issue == "Non-DV HoHs Entering PH or TH without HARP or SPDAT") %>%
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

  summary_pe$ScoredAtPHEntry <- pe$ScoredAtPHEntry %>%
    dplyr::group_by(ProjectType, AltProjectName, ScoredAtEntryDQ) %>%
    dplyr::summarise(ScoredAtEntry = sum(MeetsObjective)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(pe_summary_validation, by = c("ProjectType", "AltProjectName")) %>%
    dplyr::mutate(
      ScoredAtEntry = dplyr::if_else(is.na(ScoredAtEntry),
                                     0,
                                     ScoredAtEntry),
      ScoredAtEntryPercent = dplyr::if_else(HoHsEntered > 0,
                                            ScoredAtEntry / HoHsEntered,
                                            NA),
      ScoredAtEntryPercentJoin = dplyr::if_else(is.na(ScoredAtEntryPercent), 0, ScoredAtEntryPercent)) %>%
    dplyr::cross_join(scoring_rubric %>%
                        dplyr::filter(metric == "scored_at_ph_entry")) %>%
    dplyr::mutate(ScoredAtEntryPossible = max(points)) %>%
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
          "had a VI-SPDAT/HARP score at entry /",
          HoHsEntered,
          "heads of household who entered the project during the reporting period =",
          scales::percent(ScoredAtEntryPercent, accuracy = 0.1)
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
  pe_summary <- purrr::reduce(summary_pe, ~dplyr::full_join(.x, .y, by = c("ProjectType", "AltProjectName")))
  pe_summary_final_scoring <-
    pe_coc_funded[c("ProjectType", "AltProjectName")] %>%
    unique() %>%
    dplyr::left_join(pe_summary, by = c("ProjectType", "AltProjectName")) |>
    dplyr::distinct(AltProjectName, .keep_all = TRUE) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      TotalScore = sum(DQPoints,
                       NoIncomeAtEntryPoints,
                       ReturnToHomelessnessPoints,
                       ExitsToPHPoints,
                       ScoredAtEntryPoints,
                       MedianHHIPoints,
                       IncreasedIncomePoints,
                       IncreasedEarnedIncomePoints,
                       BenefitsAtExitPoints,
                       LHResPriorPoints,
                       na.rm = TRUE
      )
    )


  pe_final_scores <- pe_summary_final_scoring

  # pe_final_scores$HousingFirstScore[is.na(pe_final_scores$HousingFirstScore)] <- 0
  # pe_final_scores$ChronicPrioritizationScore[is.na(pe_final_scores$ChronicPrioritizationScore)] <- 0
  # pe_final_scores$PrioritizationWorkgroupScore[is.na(pe_final_scores$PrioritizationWorkgroupScore)] <- 0
  # pe_final_scores$AverageLoSPoints[is.na(pe_final_scores$AverageLoSPoints)] <- 0
  # pe_final_scores$LongTermHomelessPoints[is.na(pe_final_scores$LongTermHomelessPoints)] <- 0

  pe_final_scores <- pe_final_scores %>%
    dplyr::mutate(
      TotalScore = DQPoints +
        NoIncomeAtEntryPoints +
        ExitsToPHPoints +
        ScoredAtEntryPoints +
        MedianHHIPoints +
        IncreasedIncomePoints +
        IncreasedEarnedIncomePoints +
        BenefitsAtExitPoints +
        LHResPriorPoints +
        ReturnToHomelessnessPoints
    ) %>%
    dplyr::select(ProjectType,
                  AltProjectName,
                  dplyr::ends_with("Points"),
                  dplyr::ends_with("Score"),
                  dplyr::ends_with("Scoring"),
                  TotalScore)

  # adding in Organization Name for publishing the final ranking
  # Org Names for the combined projects have to be done manually

  Organization <- HMISdata::load_hmis_csv("Organization.csv")
  project_and_alt_project <- pe_coc_funded %>%
    dplyr::left_join(Project[c("ProjectID", "OrganizationID")], by = "ProjectID") %>%
    dplyr::left_join(Organization[c("OrganizationID", "OrganizationName")],
                     by = "OrganizationID")

  final_scores <- pe_final_scores %>%
    dplyr::select(AltProjectName, TotalScore) %>%
    dplyr::left_join(project_and_alt_project, by = c("AltProjectName" = "ProjectName")) %>%
    dplyr::select(OrganizationName, AltProjectName, TotalScore) %>%
    dplyr::arrange(dplyr::desc(TotalScore))


  exported_pe <- pe[c("ScoredAtPHEntry", "ReturnToHomelessness", "HomelessHistoryIndex", "IncreaseIncome", 
  "IncreaseEarnedIncome", "ResPrior", "BenefitsAtExit", "ExitsToPH", "EntriesNoIncome")] |>
    {\(x) {rlang::set_names(x, paste0("pe_", snakecase::to_snake_case(names(x))))}}()

  # Combine everything into one named list
all_exports <- c(
  exported_pe,
  list(pe_summary_final_scoring = pe_summary_final_scoring,
  pe_summary_validation = pe_summary_validation)
)

# Upload each one
purrr::iwalk(all_exports, \(data, name) {

HMISdata::upload_hmis_data(
    data,
    bucket = "shiny-data-cohhio",
    folder = "RME",
    file_name = paste0(name, ".parquet"),
    format = "parquet"
  )
})
}
