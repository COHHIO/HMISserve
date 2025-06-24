
get_null_names <- function(fmls = rlang::fn_fmls(), e = rlang::caller_env()) {
  names(fmls)[purrr::imap_lgl(fmls, ~exists(.y, e, mode = "NULL"))]
}

#' @title Funder_ProjectIDs
#' @description This filters for Project IDs funded by FundingSources matching `fund_regex`. See \link[HMIS]{hud_translations}\code{\$2.06.1 FundingSource(table = TRUE)} for Funding Source names.
#' @inheritParams data_quality_tables
#' @param fund_regex \code{(character)} regular expression. **Default: `"^VA"` for VA funded projects
#' @return \code{(data.frame)} with Project ID
#'

Funder_ProjectIDs <- function(Funder, fund_regex = "^VA") {
  dplyr::filter(Funder, stringr::str_detect(HMIS::hud_translations$`2.06.1 FundingSource`(Funder), fund_regex)) |>
    dplyr::select(ProjectID)
}



#' @title Filter for Current HMIS participating projects
#'
#' @inheritParams data_quality_tables
#'
#' @return \code{(data.frame)}

projects_current_hmis <- function (Project,
                                   Inventory,
                                   rm_dates) {
  Project |>
    dplyr::left_join(Inventory, by = "ProjectID") |>
    HMIS::operating_between(rm_dates$calc$data_goes_back_to, rm_dates$meta_HUDCSV$Export_End) |>
    dplyr::filter(HMISParticipationType == 1 &
                    (GrantType != "HOPWA" | is.na(GrantType))) |>
    dplyr::select(
      ProjectID,
      OrganizationID,
      OperatingStartDate,
      OperatingEndDate,
      ProjectType,
      GrantType,
      ProjectName,
      OrganizationName,
      ProjectCounty,
      ProjectRegion
    ) |>
    dplyr::distinct(ProjectID, .keep_all = TRUE)
}

make_vars <- function() {
  vars <- list()
  vars$prep <- c(
    "EnrollmentID",
    "EntryAdjust",
    "EntryDate",
    "ExitDate",
    "HouseholdID",
    "MoveInDateAdjust",
    "PersonalID",
    "ProjectID",
    "ProjectName",
    "ProjectRegion",
    "ProjectType",
    "UniqueID",
    "UserCreating"
  )

  vars$we_want <- c(vars$prep,
                    "Issue",
                    "Type",
                    "Guidance")
  vars
}

#' @title Create the data frame of Clients to Check `served_in_date_range`
#'
#' @description Create a data frame of clients served in the specified date range, including various demographic and enrollment details.
#'
#' @param projects_current_hmis \code{(data.frame)} A data frame of providers to check. See `projects_current_hmis`.
#' @param Enrollment_extra_Client_Exit_HH_CL_AaE \code{(data.frame)} Enrollment data with all additions from `load_export`.
#' @param Client \code{(data.frame)} Client data with all additions from `load_export`.
#' @param Project \code{(data.frame)} Project data with extras including Regions and GrantType, see `pe_add_regions` & `pe_add_GrantType`.
#' @param Inventory \code{(data.frame)} Inventory data.
#' @param HealthAndDV \code{(data.frame)} Health and Domestic Violence data.
#' @param vars \code{(list)} A list of variables to include.
#' @param rm_dates \code{(list)} A list containing date ranges with elements `calc$data_goes_back_to` and `meta_HUDCSV$Export_End`.
#' @param app_env \code{(environment)} Instead of providing all arguments with NULL defaults, `app_env` with all arguments saved internally can be provided.
#'
#' @return \code{(data.frame)} A data frame of clients served in the specified date range.
#' @export


served_in_date_range <- function(projects_current_hmis,
                                 Enrollment_extra_Client_Exit_HH_CL_AaE = NULL,
                                 Client = NULL, Project = NULL,
                                 Inventory = NULL,
                                 HealthAndDV = NULL,
                                 vars,
                                 rm_dates = NULL) {
  Enrollment_extra_Client_Exit_HH_CL_AaE  |>
    HMIS::served_between(rm_dates$calc$data_goes_back_to, rm_dates$meta_HUDCSV$Export_End)  |>
    dplyr::select(
      dplyr::all_of(
        c(
          "AdditionalRaceEthnicity",
          "AgeAtEntry",
          "AmIndAKNative",
          "Asian",
          "BlackAfAmerican",
          "ClientEnrolledInPATH",
          "CountyPrior",
          "CountyServed",
          "CulturallySpecific",
          "DateCreated",
          "DateDeleted",
          "DateOfEngagement",
          "DateOfPATHStatus",
          "DateToStreetESSH",
          "Destination",
          "DestinationSubsidyType",
          "DifferentIdentity",
          "DifferentIdentityText",
          "DisablingCondition",
          "DOB",
          "DOBDataQuality",
          "EnrollmentID",
          "EntryDate",
          "EntryAdjust",
          "ExitAdjust",
          "ExitDate",
          "ExpectedPHDate",
          "FirstName",
          "FundingSourceCode",
          "GenderNone",
          "HispanicLatinaeo",
          "HouseholdID",
          "LengthOfStay",
          "LengthOfStay",
          "LivingSituation",
          "LOSUnderThreshold",
          "Man",
          "MidEastNAfrican",
          "MonthsHomelessPastThreeYears",
          "MoveInDate",
          "MoveInDateAdjust",
          "NameDataQuality",
          "NativeHIPacific",
          "NonBinary",
          "NonFederalFundingSourceCode",
          "PersonalID",
          "PHTrack",
          "PreviousStreetESSH",
          "ProjectID",
          "Questioning",
          "RaceNone",
          "ReasonNotEnrolled",
          "RelationshipToHoH",
          "RentalSubsidyType",
          "SSN",
          "SSNDataQuality",
          "TimesHomelessPastThreeYears",
          "Transgender",
          "UniqueID",
          "UserCreating",
          "VeteranStatus",
          "White",
          "Woman"
        ))
    ) |>
    dplyr::inner_join(projects_current_hmis, by = "ProjectID") |>
    dplyr::filter(stringr::str_detect(ProjectName, "\\sVASH\\s?", negate = TRUE)) |>
    dplyr::filter(is.na(DateDeleted)) |>
    dplyr::left_join(
      HealthAndDV  |>
        dplyr::filter(DataCollectionStage == 1)  |>
        dplyr::select(
          EnrollmentID,
          DomesticViolenceSurvivor,
          WhenOccurred,
          CurrentlyFleeing
        ),
      by = "EnrollmentID"
    )
}

#' Filter for Enrollments in a Specific Project Type
#'
#' @param served_in_date_range \code{(data.frame)} See `served_in_date_range`
#' @param type \code{(numeric)} ProjectType. For full project type names see `HMIS::hud_translations$[["2.02.6 ProjectType"]](table = TRUE)`
#' @param has_movein Flag for whether of not client has a move in date
#'
#' @return \code{(data.frame)} with `PersonalID` for all Enrollees in the ProjectType, their `MoveInDateAdjust`, the `TimeInterval` for which they were in the Project, and the `ProjectName`
#' @export

enrolled_in <-
  function(served_in_date_range,
           type = c(
             ES = 0,
             TH = 2,
             PSH = 3,
             SO = 4,
             ServicesOnly = 6,
             Other = 7,
             SH = 8,
             PHHO = 9,
             PHHS = 10,
             DS = 11,
             HP = 12,
             RRH = 13,
             CE = 14
           )[13],
           has_movein = FALSE) {
    f_expr <- rlang::expr(ProjectType %in% type)
    if (has_movein)
      f_expr <- rlang::expr(!!f_expr & !is.na(MoveInDateAdjust))
    served_in_date_range |>
      dplyr::filter(!!f_expr) |>
      dplyr::mutate(TimeInterval = lubridate::interval(EntryDate, ExitAdjust - lubridate::days(1))) |>
      dplyr::select(PersonalID,
                    MoveInDateAdjust,
                    TimeInterval,
                    ProjectName) |>
      dplyr::distinct()
  }

# Missing UDEs ------------------------------------------------------------

#' @title Data Quality report on Missing First Names
#' @family Clarity Checks
#' @family DQ: Missing UDEs
#' @inherit data_quality_tables params return

dq_name <- function(served_in_date_range, guidance = NULL, vars = NULL) {
  served_in_date_range  |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        FirstName == "Missing" ~
          "Missing Name Data Quality",
        FirstName %in% c("DKR", "Partial") ~
          "Incomplete or Don't Know/Prefers Not to Answer Name"
      ),
      Type = dplyr::case_when(
        Issue == "Missing Name Data Quality" ~ "Error",
        Issue == "Incomplete or Don't Know/Prefers Not to Answer Name" ~ "Warning"
      ),
      Guidance = dplyr::if_else(Type == "Warning",
                                guidance$dkr_data,
                                guidance$missing_pii)
    )  |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Data quality report on Missing/Incorrect DOB
#' @family Clarity Checks
#' @family DQ: Missing UDEs
#' @inherit data_quality_tables params return
# TODO Check to ensure missing DOB are not present in imported.

dq_dob <- function(served_in_date_range,
                   guidance = NULL,
                   vars = NULL) {
  served_in_date_range |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        is.na(DOB) & DOBDataQuality %in% c(1, 2) ~ "Missing DOB",
        DOBDataQuality == 99 ~ "Missing Date of Birth Data Quality",
        DOBDataQuality %in% c(2, 8, 9) ~ "Don't Know/Prefers Not to Answer Approx. Date of Birth",
        AgeAtEntry < 0 |
          AgeAtEntry > 95 ~ "Incorrect Date of Birth or Entry Date"
      ),
      Type = dplyr::case_when(
        Issue %in% c(
          "Missing DOB",
          "Incorrect Date of Birth or Entry Date",
          "Missing Date of Birth Data Quality"
        ) ~ "Error",
        Issue ==  "Don't Know/Prefers Not to Answer Approx. Date of Birth" ~ "Warning"
      ),
      Guidance = dplyr::case_when(
        Issue == "Incorrect Date of Birth or Entry Date" ~
          "The HMIS data is indicating the client entered the project PRIOR to
      being born. Correct either the Date of Birth or the Entry Date, whichever
      is incorrect.",
        Issue %in% c("Missing DOB", "Missing Date of Birth Data Quality") ~
          guidance$missing_at_entry,
        Issue == "Don't Know/Prefers Not to Answer Approx. Date of Birth" ~
          guidance$dkr_data
      )
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Data quality report on SSN Validity
#' @family Clarity Checks
#' @family DQ: Missing UDEs

#' @inherit data_quality_tables params return

dq_ssn <- function(served_in_date_range,
                   guidance = NULL,
                   vars = NULL) {
  served_in_date_range |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        SSN == "Missing" ~ "Missing SSN",
        SSN == "Four Digits Provided" & (FundingSourceCode == 33 | NonFederalFundingSourceCode == 5) ~ "Invalid SSN",
        SSN == "Invalid" ~ "Invalid SSN",
        SSN == "DKR" ~ "Don't Know/Prefers Not to Answer SSN",
        SSN == "Incomplete" ~ "Invalid SSN"
      ),
      Type = dplyr::case_when(
        Issue %in% c("Missing SSN", "Invalid SSN") ~ "Error",
        Issue == "Don't Know/Prefers Not to Answer SSN" ~ "Warning"
      ),
      Guidance = dplyr::case_when(
        Issue == "Don't Know/Prefers Not to Answer SSN" ~ guidance$dkr_data,
        Issue == "Missing SSN" ~ guidance$missing_pii,
        Issue == "Invalid SSN" ~ guidance$invalid_ssn
      )
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Data quality report on Race data
#' @family Clarity Checks
#' @family DQ: Missing UDEs

#' @inherit data_quality_tables params return

dq_race <- function(served_in_date_range,
                    guidance = NULL,
                    vars = NULL) {
  served_in_date_range |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        RaceNone == 99 ~ "Missing Race and Ethnicity",
        RaceNone %in% c(8, 9) ~ "Don't Know/Prefers Not to Answer Race and Ethnicity"
      ),
      Type = dplyr::case_when(
        Issue == "Missing Race and Ethnicity" ~ "Error",
        Issue == "Don't Know/Prefers Not to Answer Race and Ethnicty" ~ "Warning"
      ),
      Guidance = dplyr::if_else(Type == "Warning",
                                guidance$dkr_data,
                                guidance$missing_at_entry)
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


#' @title Data quality report on Gender Data
#' @family Clarity Checks
#' @family DQ: Missing UDEs

#' @inherit data_quality_tables params return

dq_gender <- function(served_in_date_range,
                      guidance = NULL,
                      vars = NULL) {
  served_in_date_range |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        GenderNone == 99 ~ "Missing Gender",
        GenderNone %in% c(8, 9) ~ "Don't Know/Prefers Not to Answer Gender"
      ),
      Type = dplyr::case_when(
        GenderNone == 99 ~ "Error",
        GenderNone %in% c(8, 9) ~ "Warning"
      ),
      Guidance = dplyr::if_else(Type == "Warning",
                                guidance$dkr_data,
                                guidance$missing_at_entry)
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


# Household Issues --------------------------------------------------------
#' @title Find Households without adults
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Household Checks
#' @export
dq_hh_children_only <- function(served_in_date_range,
                                vars,
                                guidance = NULL) {
  served_in_date_range |>
    dplyr::filter(GrantType != "RHY" |
                    is.na(GrantType)) |> # not checking for children-only hhs for RHY
    dplyr::group_by(HouseholdID) |>
    dplyr::summarise(
      hhMembers = dplyr::n(),
      maxAge = max(AgeAtEntry),
      PersonalID = min(PersonalID)
    ) |>
    dplyr::filter(maxAge < 18) |>
    dplyr::ungroup() |>
    dplyr::left_join(served_in_date_range, by = c("PersonalID", "HouseholdID")) |>
    dplyr::mutate(Issue = "Children Only Household",
                  Type = "High Priority",
                  Guidance = guidance$hh_children_only) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Households with Missing Relationship to Head of Household
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Household Checks
#' @export
dq_hh_missing_rel_to_hoh <- function(served_in_date_range, vars, guidance = NULL) {
  hh_no_hoh <- dq_hh_no_hoh(served_in_date_range, vars, guidance)
  served_in_date_range |>
    dplyr::filter(RelationshipToHoH == 99) |>
    dplyr::anti_join(hh_no_hoh["HouseholdID"], by = "HouseholdID") |>
    dplyr::mutate(Issue = "Missing Relationship to Head of Household",
                  Type = "High Priority",
                  Guidance = guidance$hh_missing_rel_to_hoh) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Households with no Head of Household
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Household Checks
#' @export
dq_hh_no_hoh <- function(served_in_date_range, vars, guidance = NULL){
  served_in_date_range |>
    dplyr::group_by(HouseholdID) |>
    dplyr::summarise(hasHoH = any(RelationshipToHoH == 1)) |>
    dplyr::filter(!hasHoH) |>
    dplyr::select(HouseholdID) |>
    dplyr::inner_join(served_in_date_range, by = "HouseholdID") |>
    dplyr::mutate(
      Issue = "No Head of Household",
      Type = "High Priority",
      Guidance = guidance$hh_no_hoh
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find clients that are active with HoH exit
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Household Checks
#' @export
dq_hh_active_client_no_hoh <- function(served_in_date_range, vars, guidance = NULL) {
  served_in_date_range |>
    # For each HouseholdID, get both HoH status indicators
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(
      has_active_HoH = any(RelationshipToHoH == 1, na.rm = TRUE),
      is_HoH = (RelationshipToHoH == 1),
      HoH_has_exited = any(is_HoH & !is.na(ExitDate), na.rm = TRUE)
    ) |>
    # Filter for either case
    dplyr::filter(
      !is_HoH &  # Not a HoH
        is.na(ExitDate) &  # Member is still active
        # Either no active HoH OR HoH has exited
        (!has_active_HoH | HoH_has_exited == TRUE)
    ) |>
    # Add issue details
    dplyr::mutate(
      Issue = dplyr::case_when(
        (!has_active_HoH | HoH_has_exited == TRUE) ~ "Client remains active after Head of Household's exit or deletion",
        TRUE ~ NA_character_
      ),
      Type = "High Priority",
      Guidance = guidance$hh_no_hoh
    ) |>
    # Select final columns
    dplyr::select(
      dplyr::all_of(vars$we_want)
    )
}



# Missing Data at Entry ---------------------------------------------------
#' @title Find Missing Date Homeless
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_missing_approx_date_homeless <- function(served_in_date_range, vars, guidance = NULL, rm_dates = NULL) {
  missing_approx_date_homeless <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      EnrollmentID,
      ProjectID,
      AgeAtEntry,
      RelationshipToHoH,
      LOSUnderThreshold,
      DateToStreetESSH,
      PreviousStreetESSH
    ) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    EntryDate >= rm_dates$hc$prior_living_situation_required &
                    is.na(DateToStreetESSH) &
                    ((LOSUnderThreshold == 1 & PreviousStreetESSH == 1 & ProjectType %in% c(2, 3, 6, 7, 9:14)) | ProjectType %in% c(0, 1, 4, 8))
    ) |>
    dplyr::mutate(Issue = "Missing Approximate Date Homeless",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))
    return(missing_approx_date_homeless)
}

#' @title Find Missing Length of Time Homeless questions for Emergency Shelters and Safe Havens
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_missing_previous_street_ESSH <- function(served_in_date_range, vars, guidance = NULL, rm_dates = NULL) {
  missing_previous_street_ESSH <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      DateToStreetESSH,
      PreviousStreetESSH,
      LOSUnderThreshold
    ) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    EntryDate >= rm_dates$hc$prior_living_situation_required &
                    is.na(PreviousStreetESSH) &
                    LOSUnderThreshold == 1
    ) |>
    dplyr::mutate(Issue = "Missing Previously From Street, ES, or SH (Length of Time Homeless questions)",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

    return(missing_previous_street_ESSH)
}

#' @title Find Missing Prior Living Situation
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_missing_prior_living_situation <- function(served_in_date_range, vars, guidance = NULL) {
  missing_residence_prior <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  AgeAtEntry,
                  RelationshipToHoH,
                  LivingSituation) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    (is.na(LivingSituation) | LivingSituation == 99)) |>
    dplyr::mutate(Issue = "Missing Prior Living Situation",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

    return(missing_residence_prior)
}


#' @title Find Don't Know/Prefers Not to Answer Prior Living Situation
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_dkr_prior_living_situation <- function(served_in_date_range, vars, guidance = NULL) {
  dkr_residence_prior <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  AgeAtEntry,
                  RelationshipToHoH,
                  LivingSituation) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    LivingSituation %in% c(8, 9)) |>
    dplyr::mutate(Issue = "Don't Know/Prefers Not to Answer Prior Living Situation",
                  Type = "Warning",
                  Guidance = guidance$dkr_data) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  return(dkr_residence_prior)
}



#' @title Find Don't Know/Prefers Not to Answer Length of Stay
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_dkr_LoS <- function(served_in_date_range, vars, guidance = NULL) {
  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  AgeAtEntry,
                  RelationshipToHoH,
                  LengthOfStay) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    LengthOfStay %in% c(8, 9)) |>
    dplyr::mutate(Issue = "Don't Know/Prefers Not to Answer Length of Stay",
                  Type = "Warning",
                  Guidance = guidance$dkr_data) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing Months or Times Homeless
#' @inherit data_quality_tables params return
#' @family DQ: Missing Data at Entry
#' @export
dq_missing_months_times_homeless <- function(served_in_date_range, vars, guidance = NULL, rm_dates = NULL) {
  missing_months_times_homeless <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      MonthsHomelessPastThreeYears,
      TimesHomelessPastThreeYears
    ) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    EntryDate >= rm_dates$hc$prior_living_situation_required &
                    ProjectType %in% c(0, 1, 4, 8) &
                    (
                      is.na(MonthsHomelessPastThreeYears) |
                        is.na(TimesHomelessPastThreeYears) |
                        MonthsHomelessPastThreeYears == 99 |
                        TimesHomelessPastThreeYears == 99
                    )
    ) |>
    dplyr::mutate(Issue = "Missing Months or Times Homeless",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  return(missing_months_times_homeless)
}

#' @title Find Where Client is Homeless After Entry
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_date_homeless_after_entry <- function(served_in_date_range, vars, rm_dates = NULL, guidance = NULL) {
  out <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      MonthsHomelessPastThreeYears,
      TimesHomelessPastThreeYears,
      DateToStreetESSH
    ) |>
    dplyr::filter(
      ProjectType != 12 &
        (RelationshipToHoH == 1 | AgeAtEntry > 17) &
        EntryDate >= rm_dates$hc$prior_living_situation_required &
        !is.na(DateToStreetESSH)
    ) |>
    dplyr::filter(EntryDate < DateToStreetESSH) |>
    dplyr::mutate(Issue = "Homelessness Start Date Later Than Entry",
                  Type = "Warning",
                  Guidance = guidance$date_homeless_after_entry) |>
    dplyr::filter(!is.na(Guidance)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}

#' @title Find Number of Months Homeless Can Be Determined
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_months_homeless_tbd <- function(served_in_date_range, vars, rm_dates = NULL, guidance = NULL) {
  out <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      MonthsHomelessPastThreeYears,
      TimesHomelessPastThreeYears,
      DateToStreetESSH
    ) |>
    dplyr::filter(
      ProjectType != 12 &
        (RelationshipToHoH == 1 | AgeAtEntry > 17) &
        EntryDate >= rm_dates$hc$prior_living_situation_required &
        !is.na(DateToStreetESSH)
    ) |>
    dplyr::filter(MonthsHomelessPastThreeYears < 100 & TimesHomelessPastThreeYears == 1) |>
    dplyr::mutate(Issue = "Number of Months Homeless Can Be Determined",
                  Type = "Warning",
                  Guidance = guidance$months_homeless_tbd) |>
    dplyr::filter(!is.na(Guidance)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}

#' @title Find Invalid Months or Times Homeless Entries
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_invalid_months_times_homeless <- function(served_in_date_range, vars, rm_dates = NULL, guidance = NULL) {
  out <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      MonthsHomelessPastThreeYears,
      TimesHomelessPastThreeYears,
      DateToStreetESSH
    ) |>
    dplyr::filter(
      ProjectType != 12 &
        (RelationshipToHoH == 1 | AgeAtEntry > 17) &
        EntryDate >= rm_dates$hc$prior_living_situation_required &
        !is.na(DateToStreetESSH)
    ) |>
    dplyr::mutate(
      MonthHomelessnessBegan = lubridate::floor_date(DateToStreetESSH, "month"),
      MonthEnteredProgram = lubridate::floor_date(EntryDate, "month"),
      MonthDiff = lubridate::time_length(lubridate::interval(MonthHomelessnessBegan, MonthEnteredProgram), "months") + 1,
      MonthDiff = dplyr::if_else(MonthDiff >= 13, 13, MonthDiff) + 100,
      DateMonthsMismatch = dplyr::if_else(MonthsHomelessPastThreeYears - MonthDiff != 0 & TimesHomelessPastThreeYears == 1,
                                          1,
                                          0
      )) |>
    dplyr::filter(DateMonthsMismatch == 1 & TimesHomelessPastThreeYears == 1) |>
    dplyr::mutate(Issue = "Invalid Homelessness Start Date/Number of Months Homeless",
                  Type = "Warning",
                  Guidance = guidance$months_homeless_inconsistent) |>
    dplyr::filter(!is.na(Guidance)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}

#' @title Find Missing Living Situation
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_missing_living_situation <- function(served_in_date_range, vars, rm_dates = NULL, guidance = NULL) {
  served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      LivingSituation,
      LengthOfStay,
      LOSUnderThreshold,
      PreviousStreetESSH,
      DateToStreetESSH,
      MonthsHomelessPastThreeYears,
      TimesHomelessPastThreeYears
    ) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    EntryDate >= rm_dates$hc$prior_living_situation_required &
                    # not req'd prior to this
                    ProjectType %in% c(2, 3, 6, 9, 10, 12, 13) &
                    (
                      (
                        LivingSituation %in% c(215, 206, 207, 24, 204, 205) &
                          LengthOfStay %in% c(2, 3, 10, 11) &
                          (is.na(LOSUnderThreshold) |
                             is.na(PreviousStreetESSH))
                      ) |
                        (
                          LivingSituation %in% c(2, 3, 312, 313, 314, 215, 419,
                                                 420, 421, 422, 423, 225, 426) &
                            LengthOfStay %in% c(10, 11) &
                            (is.na(LOSUnderThreshold) |
                               is.na(PreviousStreetESSH))
                        )
                    )
    ) |>
    dplyr::mutate(Issue = "Incomplete Living Situation Data",
                  Type = "Error",
                  Guidance = guidance$missing_living_situation
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


#' @title Find Don't Know/Prefers Not to Answer Months or Times Homeless
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Missing Data at Entry
#' @export
dq_dkr_months_times_homeless <- function(served_in_date_range, vars, rm_dates = NULL, guidance = NULL) {
  served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      MonthsHomelessPastThreeYears,
      TimesHomelessPastThreeYears
    ) |>
    dplyr::filter((RelationshipToHoH == 1 | AgeAtEntry > 17) &
                    EntryDate >= rm_dates$hc$prior_living_situation_required &
                    (
                      MonthsHomelessPastThreeYears %in% c(8, 9) |
                        TimesHomelessPastThreeYears %in% c(8, 9)
                    )
    ) |>
    dplyr::mutate(Issue = "Don't Know/Prefers Not to Answer Months or Times Homeless",
                  Type = "Warning",
                  Guidance = guidance$dkr_data) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing Disabilities and Conflicting Disability of Long Duration
#' @family Clarity Checks
#' @inherit data_quality_tables params return
#' @family DQ: Missing Data at Entry
#' @export
dq_detail_missing_disabilities <- function(served_in_date_range, Disabilities, vars, guidance = NULL) {
  missing_disabilities <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  AgeAtEntry,
                  RelationshipToHoH,
                  DisablingCondition) |>
    dplyr::filter(DisablingCondition == 99 |
                    is.na(DisablingCondition)) |>
    dplyr::mutate(Issue = "Missing Disabling Condition",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))


  smallDisabilities <- Disabilities |>
    dplyr::filter(DataCollectionStage == 1 &
                    ((DisabilityType == 10 &
                        DisabilityResponse %in% c(1:3)) |
                       (DisabilityType != 10 & DisabilityResponse == 1)
                    )) |>
    dplyr::mutate(
      IndefiniteAndImpairs =
        dplyr::case_when(
          DisabilityType %in% c(6L, 8L) ~ 1L,
          TRUE ~ IndefiniteAndImpairs)
    ) |>
    dplyr::select(
      PersonalID,
      DisabilitiesID,
      EnrollmentID,
      InformationDate,
      DisabilityType,
      IndefiniteAndImpairs
    )

  # Developmental & HIV/AIDS get automatically IndefiniteAndImpairs = 1 per FY2020

  conflicting_disabilities <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  EnrollmentID,
                  AgeAtEntry,
                  RelationshipToHoH,
                  DisablingCondition,
                  GrantType) |>
    dplyr::left_join(
      smallDisabilities |>
        dplyr::filter(IndefiniteAndImpairs == 1L),
      by = c("PersonalID", "EnrollmentID")
    ) |>
    dplyr::filter((DisablingCondition == 0 & !is.na(DisabilitiesID)) |
                    (DisablingCondition == 1 & is.na(DisabilitiesID) & GrantType != "SSVF")) |>
    dplyr::mutate(
      Issue = "Conflicting Disability of Long Duration yes/no",
      Type = "Error",
      Guidance = guidance$conflicting_disability
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  out <- dplyr::bind_rows(missing_disabilities, conflicting_disabilities)
  return(out)
}

#' @title Find Clients in Mahoning with 60 Days elapsed in Coordinated Entry
#' @family Clarity Checks
#' @inherit data_quality_tables params return
#' @export

dq_mahoning_ce_60_days <- function(served_in_date_range, mahoning_projects, vars, guidance = NULL) {
  mahoning_ce <- mahoning_projects[stringr::str_detect(names(mahoning_projects), "Coordinated Entry")]
  served_in_date_range |>
    dplyr::filter(ProjectID %in% mahoning_ce &
                    EntryDate <= lubridate::today() - lubridate::days(60) &
                    is.na(ExitDate)) |>
    dplyr::mutate(
      Issue = "60 Days in Mahoning Coordinated Entry",
      Type = "Warning",
      Guidance = guidance$mahoning_60
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Clients with Extremely Long Stays
#' @family Clarity Checks
#' @inherit data_quality_tables params return
#' @export
dq_th_stayers_bos <- function(served_in_date_range, mahoning_projects, vars, guidance = NULL) {
  th_stayers_bos <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate))) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 2 &
                    !ProjectID %in% c(mahoning_projects))

  th_stayers_mah <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate))) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 2 &
                    ProjectID %in% c(mahoning_projects))

  Top2_TH_bos <- subset(th_stayers_bos, Days > stats::quantile(Days, prob = 1 - 2 / 100))
  Top2_TH_mah <- subset(th_stayers_mah, Days > stats::quantile(Days, prob = 1 - 2 / 100))

  rrh_stayers_bos <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 13 &
                    !ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  rrh_stayers_mah <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 13 &
                    ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  Top2_RRH_bos <- subset(rrh_stayers_bos, Days > stats::quantile(Days, prob = 1 - 2 / 100))
  Top2_RRH_mah <- subset(rrh_stayers_mah, Days > stats::quantile(Days, prob = 1 - 2 / 100))

  es_stayers_bos <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType %in% c(0,1) &
                    !ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  es_stayers_mah <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType %in% c(0, 1) &
                    ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  Top2_ES_bos <- subset(es_stayers_bos, Days > stats::quantile(Days, prob = 1 - 2 / 100))
  Top2_ES_mah <- subset(es_stayers_mah, Days > stats::quantile(Days, prob = 1 - 2 / 100))

  psh_stayers_bos <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 3 &
                    !ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  psh_stayers_mah <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 3 &
                    ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  Top1_PSH_bos <- subset(psh_stayers_bos, Days > stats::quantile(Days, prob = 1 - 1 / 100))
  Top1_PSH_mah <- subset(psh_stayers_mah, Days > stats::quantile(Days, prob = 1 - 1 / 100))

  hp_stayers_bos <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 12 &
                    !ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  hp_stayers_mah <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), ProjectID) |>
    dplyr::filter(is.na(ExitDate) &
                    ProjectType == 12 &
                    ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(Days = as.numeric(difftime(lubridate::today(), EntryDate)))

  Top5_HP_bos <- subset(hp_stayers_bos, Days > stats::quantile(Days, prob = 1 - 5 / 100))
  Top10_HP_mah <- subset(hp_stayers_mah, Days > stats::quantile(Days, prob = 90 / 100))

  rbind(Top1_PSH_bos,
        Top2_ES_bos,
        Top2_RRH_bos,
        Top2_TH_bos,
        Top5_HP_bos,
        Top1_PSH_mah,
        Top2_ES_mah,
        Top2_RRH_mah,
        Top2_TH_mah,
        Top10_HP_mah) |>
    dplyr::mutate(
      Issue = "Extremely Long Stayer",
      Type = "Warning",
      Guidance =  guidance$th_stayers_bos
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

# Incorrect Destination ---------------------------------------------------

#' @title Find Incorrect Exits in RRH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export
dq_rrh_check_exit_destination <- function(served_in_date_range, vars, guidance = NULL) {
  enrolled_in_type <- enrolled_in(served_in_date_range, type = 13, has_movein = TRUE) |> dplyr::select(PersonalID, MoveInDateAdjust)

  served_in_date_range |>
    dplyr::select(PersonalID,
                  ProjectType,
                  dplyr::all_of(vars$prep),
                  ExitDate,
                  Destination,
                  RentalSubsidyType) |>
    dplyr::left_join(enrolled_in_type, by = "PersonalID", suffix = c("", "_rrh")) |>
    dplyr::filter(ProjectType != 13 &
                    ExitDate == MoveInDateAdjust_rrh &
                    !(Destination == 435 & RentalSubsidyType == 431)) |>
    dplyr::mutate(
      Issue = "Maybe Incorrect Exit Destination (did you mean 'Rental by client, with RRH...'?)",
      Type = "Warning",
      Guidance = guidance$rrh_check_exit_destination
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Possibly Incorrect Exits in PSH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export
dq_psh_check_exit_destination <- function(served_in_date_range, vars, guidance = NULL) {
  enrolled_in_type <- enrolled_in(served_in_date_range, type = c(3, 9), TRUE)

  served_in_date_range |>
    dplyr::left_join(enrolled_in_type, by = "PersonalID", suffix = c("", "_psh")) |>
    dplyr::filter(!ProjectType %in% c(3, 9) &
                    lubridate::`%within%`(ExitAdjust, TimeInterval)  &
                    (!DestinationSubsidyType %in% c(419, 439, 440)) &
                    Destination != 426
    ) |>
    dplyr::mutate(
      Issue = "Check Exit Destination",
      Type = "Warning",
      Guidance = guidance$psh_check_exit) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Incorrect Exits in PSH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export
dq_psh_incorrect_destination <- function(served_in_date_range, vars, guidance = NULL) {
  enrolled_in_type <- enrolled_in(served_in_date_range, type = c(3, 9), TRUE)
  served_in_date_range |>
    dplyr::left_join(enrolled_in_type, by = "PersonalID", suffix = c("", "_psh")) |>
    dplyr::filter(!ProjectType %in% c(3, 9) &
                    ExitDate == MoveInDateAdjust_psh  &
                    (!DestinationSubsidyType %in% c(419, 439, 440)) &
                    Destination != 426
    ) |>
    dplyr::mutate(
      Issue = "Incorrect Exit Destination",
      Type = "Error",
      Guidance = guidance$psh_incorrect_destination) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Incorrect Exits in TH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export

dq_th_check_exit_destination <- function(served_in_date_range, vars, guidance = NULL) {
  enrolled_in_type <- enrolled_in(served_in_date_range, type = 2)
  served_in_date_range |>
    dplyr::left_join(enrolled_in_type, by = "PersonalID", suffix = c("", "_th")) |>
    dplyr::filter(ProjectType != 2 &
                    lubridate::`%within%`(ExitAdjust, TimeInterval) &
                    Destination != 302) |>
    dplyr::mutate(
      Issue = "Incorrect Exit Destination (should be \"Transitional housing...\")",
      Type = "Error",
      Guidance = guidance$th_check_exit_destination
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Incorrect Exits in SH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export

dq_sh_check_exit_destination <- function(served_in_date_range, vars, guidance = NULL) {
  enrolled_in_type <- enrolled_in(served_in_date_range, type = 8)
  served_in_date_range |>
    dplyr::left_join(enrolled_in_type, by = "PersonalID", suffix = c("", "_sh")) |>
    dplyr::filter(ProjectType != 8 &
                    lubridate::`%within%`(ExitAdjust, TimeInterval) &
                    Destination != 118) |>
    dplyr::mutate(
      Issue = "Incorrect Exit Destination (should be \"Safe Haven\")",
      Type = "Error",
      Guidance = guidance$sh_check_exit_destination
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

# Missing Project Stay or Incorrect Destination ---------------------------
#' @title Find Missing Project Stay or Incorrect Destination for RRH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export

dq_rrh_missing_project_stay <- function(served_in_date_range, vars, guidance = NULL) {
  served_in_date_range |>
    dplyr::filter((Destination == 435 & RentalSubsidyType == 431)) |>
    dplyr::anti_join(enrolled_in(served_in_date_range, type = 13), by = "PersonalID") |>
    dplyr::mutate(
      Issue = "Missing RRH Project Stay or Incorrect Destination",
      Type = "Warning",
      Guidance = guidance$rrh_missing_stay
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing Project Stay or Incorrect Destination for PSH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export
dq_psh_missing_project_stay <- function(served_in_date_range, vars, guidance = NULL) {
  served_in_date_range |>
    dplyr::filter(Destination == 3) |>
    dplyr::anti_join(enrolled_in(served_in_date_range, type = c(3,9), TRUE), by = "PersonalID") |>
    dplyr::mutate(
      Issue = "Missing PSH Project Stay or Incorrect Destination",
      Type = "Warning",
      Guidance = guidance$psh_missing_stay
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing Project Stay or Incorrect Destination for TH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export

dq_th_missing_project_stay <- function(served_in_date_range, vars, guidance = NULL) {
  served_in_date_range |>
    dplyr::filter(Destination == 2) |>
    dplyr::anti_join(enrolled_in(served_in_date_range, type = 2), by = "PersonalID") |>
    dplyr::mutate(
      Issue = "Missing TH Project Stay or Incorrect Destination",
      Type = "Warning",
      Guidance = guidance$th_missing_stay
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing Project Stay or Incorrect Destination for SH
#' @inherit data_quality_tables params return
#' @family Clarity Checks
#' @family DQ: Incorrect Destinations
#' @export
dq_sh_missing_project_stay <- function(served_in_date_range, vars, guidance = NULL) {
  served_in_date_range |>
    dplyr::filter(Destination == 118) |>
    dplyr::anti_join(enrolled_in(served_in_date_range, type = 8), by = "PersonalID") |>
    dplyr::mutate(
      Issue = "Missing Safe Haven Project Stay or Incorrect Destination",
      Type = "Warning",
      Guidance = guidance$sh_missing_project_stay
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing County Served
#' @family Clarity Checks
#' @inherit data_quality_tables params return
#' @export

dq_missing_county_served <- function(served_in_date_range, mahoning_projects, vars, guidance = NULL) {
  out <- served_in_date_range |>
    dplyr::filter(is.na(CountyServed) & !ProjectID %in% c(mahoning_projects)) |>
    dplyr::mutate(
      Issue = "Missing County Served",
      Type = "Error",
      Guidance = guidance$missing_county_served
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}

#' @title Find Missing County Prior
#' @family Clarity Checks
#' @inherit data_quality_tables params return
#' @export

dq_missing_county_prior <- function(served_in_date_range, mahoning_projects, vars) {
  out <- served_in_date_range |>
    dplyr::filter(is.na(CountyPrior) & !ProjectID %in% c(mahoning_projects) &
                    (AgeAtEntry > 17 |
                       is.na(AgeAtEntry))) |>
    dplyr::mutate(Issue = "Missing County of Prior Residence",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}

# Check Eligibility, Project Type, Residence Prior ------------------------
#' @title Check Eligibility
#' @description The Residence Prior may suggest that the project is serving ineligible households, the household was entered into the wrong project, or the Residence Prior at Entry is incorrect.
#' @family Clarity Checks
#' @family DQ: Check Eligibility
#' @inherit data_quality_tables params return
#'
#' @export
dq_check_eligibility <- function(served_in_date_range, mahoning_projects, vars, rm_dates) {
  check_eligibility <- served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      RelationshipToHoH,
      LivingSituation,
      LengthOfStay,
      LOSUnderThreshold,
      PreviousStreetESSH,
      GrantType
    ) |>
    dplyr::filter(
      RelationshipToHoH == 1 &
        AgeAtEntry > 17 &
        EntryDate > rm_dates$hc$check_eligibility_back_to &
        (ProjectType %in% c(3, 4, 8, 9, 10, 12, 13) |
           (
             ProjectType == 2 & (is.na(GrantType) | GrantType != "RHY")
           )) &
        (
          (ProjectType %in% c(2, 3, 9, 10, 13) &
             # PTCs that require LH status
             (
               is.na(LivingSituation) |
                 (
                   LivingSituation %in% c(204:207, 215, 225, 327, 329) & # institution
                     (
                       !LengthOfStay %in% c(2, 3, 10, 11) | # <90 days
                         is.na(LengthOfStay) |
                         PreviousStreetESSH == 0 | # LH prior
                         is.na(PreviousStreetESSH)
                     )
                 ) |
                 (
                   LivingSituation %in% c(3, 410, 411, 314, 419:423, 428, 431, 435, 436) &
                     # not homeless
                     (
                       !LengthOfStay %in% c(10, 11) |  # <1 week
                         is.na(LengthOfStay) |
                         PreviousStreetESSH == 0 | # LH prior
                         is.na(PreviousStreetESSH)
                     )
                 )
             )) |
            (ProjectType == 12 &
               LivingSituation %in% c(116, 101, 118)) |
            (ProjectType %in% c(8, 4) & # Safe Haven and Outreach
               LivingSituation != 116) # unsheltered only
        )
    )


  out <- check_eligibility |>
    dplyr::mutate(
      LivingSituation = HMIS::hud_translations$`3.12.1 Living Situation Option List`(LivingSituation),
      LengthOfStay = HMIS::hud_translations$`3.917.2 LengthOfStay`(LengthOfStay)
    ) |>
    dplyr::mutate(
      Issue = "Check Eligibility",
      Type = "Warning",
      Guidance = guidance$check_eligibility
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want), PreviousStreetESSH, LengthOfStay, LivingSituation)

  return(out)
}

#' @title Rent Payment Made, No Move-In Date
#' @description This client does not have a valid Move-In Date, but there is at least one rent/deposit payment Service Transaction recorded for this program.  Until a Move-In Date is entered, this client will continue to be counted as literally homeless while in your program. Move-in dates must be on or after the Entry Date. If a client is housed then returns to homelessness while in your program, they need to be exited from their original Entry and re-entered in a new one that has no Move-In Date until they are re-housed.
#' @inherit data_quality_tables params return
#' @param Services_enroll_extras Custom data frame with Services and Enrollments
#' @family Clarity Checks
#' @family DQ: Check Eligibility
#' @export
dq_services_rent_paid_no_move_in <- function(served_in_date_range, Services_enroll_extras, vars) {
  housing_regex <- c("Rental" , "Security", "Utility") |> {\(x) {paste0("(?:^", x, ")")}}() |> paste0(collapse = "|")
  served_in_date_range |>
    dplyr::filter(is.na(MoveInDateAdjust) &
                    RelationshipToHoH == 1 &
                    ProjectType %in% c(3, 9, 13)) |>
    dplyr::inner_join(Services_enroll_extras |>
                        dplyr::filter(
                          stringr::str_detect(ServiceItemName,  housing_regex)),
                      by = UU::common_names(served_in_date_range, Services_enroll_extras)) |>
    dplyr::mutate(
      Issue = "Housing-adjacent Payment Made, No Move-In Date",
      Type = "Error",
      Guidance = guidance$services_rent_paid_no_move_in
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Missing Destination
#' @description It is widely understood that not every client will complete an exit interview, especially for high-volume emergency shelters. A few warnings for Missing Destination is no cause for concern, but if there is a large number this will surface these errors.
#' @family Clarity Checks
#' @family DQ: Check Eligibility
#' @inherit data_quality_tables params return
#' @export

dq_missing_destination <- function(served_in_date_range,  mahoning_projects, vars) {
  out <- served_in_date_range |>
    dplyr::filter(!is.na(ExitDate) &
                    (is.na(Destination) | Destination %in% c(99, 30))) |>
    dplyr::mutate(
      Issue = "Missing Destination",
      Type = "Warning",
      Guidance = guidance$missing_destination
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}

#' @title Don't Know Prefers Not to Answer Destination
#' @family Clarity Checks
#' @family DQ: Check Eligibility
#' @inherit data_quality_tables params return
#' @export
dq_dkr_destination <- function(served_in_date_range,
                               vars,
                               guidance) {
  served_in_date_range |>
    dplyr::filter(Destination %in% c(8, 9)) |>
    dplyr::mutate(Issue = "Don't Know/Prefers Not to Answer Destination",
                  Type = "Warning",
                  Guidance = guidance$dkr_data) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

# Missing PATH Data -------------------------------------------------------


#' @title Return a subset of Project data
#' @param Project \code{(data.frame)} of Project with additional features added in `load_export`
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @return \code{(data.frame)} with ProjectID, ProjectName, ProjectCounty

dqu_project_small <- function(Project) {
  Project |> dplyr::select(ProjectID,
                           ProjectName,
                           ProjectCounty)
}



#' @title PATH: Missing Residence Prior Length of Stay
#' @inherit data_quality_tables params return
#' @family DQ: Path Checks
#' @export
dq_path_missing_los_res_prior <- function(served_in_date_range, vars, guidance) {
  served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      ProjectID,
      AgeAtEntry,
      ClientEnrolledInPATH,
      LengthOfStay
    )  |>
    dplyr::filter(AgeAtEntry > 17 &
                    ClientEnrolledInPATH == 1 &
                    (is.na(LengthOfStay) | LengthOfStay == 99)) |>
    dplyr::mutate(Issue = "Missing Residence Prior Length of Stay (PATH)",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


#' @title PATH: Status at Exit Missing or Incomplete
#' @details Engagement at Exit & adult, PATH-enrolled, Date of Engagement is null -> error
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @inherit data_quality_tables params return
#' @export
dq_path_no_status_at_exit <- function(served_in_date_range, vars,  guidance) {
  served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      GrantType,
      AgeAtEntry,
      ClientEnrolledInPATH,
      DateOfPATHStatus,
      ReasonNotEnrolled
    ) |>
    dplyr::filter(GrantType == "PATH" &
                    !is.na(ExitDate) &
                    AgeAtEntry > 17 &
                    (
                      is.na(ClientEnrolledInPATH) |
                        is.na(DateOfPATHStatus) |
                        (ClientEnrolledInPATH == 0 &
                           is.na(ReasonNotEnrolled))
                    )) |>
    dplyr::mutate(Issue = "PATH Status at Exit Missing or Incomplete",
                  Type = "Error",
                  Guidance = guidance$missing_at_exit) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}



#' @title PATH: Missing Date of PATH Status
#' @description Users must indicate the PATH Status Date for any adult enrolled in PATH.
#' @family DQ: Path Checks
#' @family Clarity Checks
#' @inherit data_quality_tables params return
#' @details Status Determination at Exit &adult, PATH-Enrolled is not null & Date of Status is null -> error
#' @export

dq_path_status_determination <- function(served_in_date_range, vars) {
  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  AgeAtEntry,
                  GrantType,
                  ClientEnrolledInPATH,
                  DateOfPATHStatus) |>
    dplyr::filter(GrantType == "PATH" &
                    AgeAtEntry > 17 &
                    !is.na(ClientEnrolledInPATH) &
                    is.na(DateOfPATHStatus)
    )  |>
    dplyr::mutate(Issue = "Missing Date of PATH Status",
                  Type = "Error",
                  Guidance = guidance$path_status) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}



#' @title PATH: Missing PATH Enrollment at Exit
#' @description Users must indicate the PATH Enrollment Date at Entry, Exit when creating an Interim
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @inherit data_quality_tables params return
#' @details PATH Enrolled at Exit & adult & PATH Enrolled null or DNC -> error
#' @export
dq_path_enrolled_missing <- function(served_in_date_range, vars) {
  out <- served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep), AgeAtEntry, ClientEnrolledInPATH, GrantType) |>
    dplyr::filter(GrantType == "PATH" &
                    !is.na(ExitDate) &
                    AgeAtEntry > 17 &
                    (ClientEnrolledInPATH == 99 |
                       is.na(ClientEnrolledInPATH))
    ) |>
    dplyr::mutate(
      Issue = "Missing PATH Enrollment at Exit",
      Type = "Error",
      Guidance = guidance$path_enrolled_missing
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}



#' @title PATH: Missing Reason Not PATH Enrolled
#' @description The user has indicated the household was not enrolled into PATH, but no reason was selected.
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @inherit data_quality_tables params return
#' @details adult & PATH Enrolled = No & Reason is null -> error
#' @export
dq_path_reason_missing <- function(served_in_date_range, vars) {
  served_in_date_range |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      ClientEnrolledInPATH,
      ReasonNotEnrolled,
      ProjectType,
      GrantType
    ) |>
    dplyr::filter(GrantType == "PATH" &
                    AgeAtEntry > 17 &
                    ClientEnrolledInPATH == 0 &
                    is.na(ReasonNotEnrolled)) |>
    dplyr::mutate(
      Issue = "Missing Reason Not PATH Enrolled",
      Type = "Error",
      Guidance = guidance$path_reason_missing
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}



#' @title PATH: Missing Connection with SOAR at Exit
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @inherit data_quality_tables params return
#' @details adult & Connection w/ SOAR is null or DNC -> error
#' @export
dq_SOAR_missing_at_exit <- function(served_in_date_range, IncomeBenefits, vars, guidance) {
  smallIncomeSOAR <- IncomeBenefits |>
    dplyr::select(PersonalID,
                  EnrollmentID,
                  ConnectionWithSOAR,
                  DataCollectionStage) |>
    dplyr::filter(DataCollectionStage == 3)

  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  EnrollmentID,
                  AgeAtEntry,
                  ClientEnrolledInPATH,
                  GrantType) |>
    dplyr::left_join(smallIncomeSOAR, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::filter(GrantType %in% c("PATH", "SSVF") &
                    AgeAtEntry > 17 &
                    DataCollectionStage == 3 &
                    is.na(ConnectionWithSOAR)) |>
    dplyr::mutate(Issue = "Missing Connection with SOAR at Exit",
                  Type = "Error",
                  Guidance = guidance$missing_at_exit) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


#' @title PATH: Missing PATH Contact
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @description  Every adult or Head of Household must have a Living Situation contact record. If you see a record there but there is no Date of Contact, saving the Date of Contact will correct this issue. This is a high priority DQ issue.
#' @inherit data_quality_tables params return
#' @param Contacts \code{(data.frame)} From the HUD CSV Export
#' @details client is adult/hoh and has no contact record in the EE -> error
#' @export
dq_missing_path_contact <- function(served_in_date_range, Contacts, rm_dates, vars) {
  ## this is a high priority data quality issue
  ## if the contact was an "Outreach" record after 10/1/2019, it is being
  ## filtered out because they should be using CLS subs past that date.
  small_contacts <-  Contacts |>
    dplyr::mutate_at(dplyr::vars("UniqueID", "PersonalID", "EnrollmentID"), as.character) |>
    dplyr::left_join(served_in_date_range, by = UU::common_names(Contacts, served_in_date_range)) |>
    dplyr::filter(
      ContactDate >= EntryDate &
        (ContactDate <= (ExitAdjust %|% (Sys.Date() + 1)))
    ) |>
    dplyr::group_by(PersonalID, ProjectName, EntryDate, ExitDate) |>
    dplyr::summarise(ContactCount = dplyr::n(), .groups = "drop")

  dqm <- served_in_date_range |>
    dplyr::filter(GrantType == "PATH" &
                    (AgeAtEntry > 17 |
                       RelationshipToHoH == 1)) |>
    dplyr::select(dplyr::all_of(vars$prep)) |>
    dplyr::left_join(small_contacts,
                     by = c("PersonalID",
                            "ProjectName",
                            "EntryDate",
                            "ExitDate")) |>
    dplyr::filter(is.na(ContactCount)) |>
    dplyr::mutate(
      Issue = "Missing PATH Contact",
      Type = "High Priority",
      Guidance = guidance$missing_path_contact
    )  |>
    dplyr::select(dplyr::all_of(vars$we_want))

  return(dqm)
}

#' @title PATH: Incorrect PATH Contact Date
#' @family Clarity Checks
#' @family DQ: Path Checks
#' @description Every adult or head of household should have a Living Situation contact record where the Contact Date matches the Entry Date. This would represent the initial contact made with the client.
#' @inherit data_quality_tables params return
#' @param Contacts \code{(data.frame)} From the HUD CSV Export
#' @details client is adult/hoh, has a contact record, and the first record in the EE does not equal the Entry Date ->  error
#' @export

dq_incorrect_path_contact_date <- function(served_in_date_range, Contacts, rm_dates, vars) {
  first_contact <- Contacts |>
    dplyr::filter(ContactDate < rm_dates$hc$outreach_to_cls) |>
    dplyr::mutate(PersonalID = as.character(PersonalID)) |>
    dplyr::left_join(served_in_date_range |> dplyr::mutate(PersonalID = as.character(PersonalID)),
                     by = "PersonalID") |>
    dplyr::select(PersonalID, EntryDate, ExitAdjust, ExitDate, ContactDate, ProjectName,
                  EntryDate, ExitAdjust) |>
    dplyr::filter(ContactDate >= EntryDate &
                    ContactDate <= ExitAdjust) |>
    dplyr::group_by(PersonalID, ProjectName, EntryDate, ExitDate) |>
    dplyr::arrange(ContactDate) |>
    dplyr::slice(1L)

  incorrect_path_contact_date <- served_in_date_range |>
    dplyr::filter(GrantType == "PATH" &
                    (AgeAtEntry > 17 |
                       RelationshipToHoH == 1)) |>
    dplyr::select(dplyr::all_of(vars$prep)) |>
    dplyr::inner_join(first_contact, by = c("PersonalID",
                                            "ProjectName",
                                            "EntryDate",
                                            "ExitDate")) |>
    dplyr::filter(ContactDate != EntryDate) |>
    dplyr::mutate(
      Issue = "No PATH Contact Entered at Entry",
      Type = "Error",
      Guidance = guidance$incorrect_path_contact_date
    ) |>
    dplyr::mutate(PersonalID = as.character(PersonalID)) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  return(incorrect_path_contact_date)
}

# Entry Exits ------------------------------------------------------

#' @title Find Duplicate EEs
#' @family Clarity Checks
#' @family DQ: EE Checks
#' @description Users sometimes create this error when they forget to click into a program stay by using the Entry pencil, and instead they click \"Add Entry/Exit\" each time. To correct, EDA to the project the Entry/Exit belongs to, navigate to the Entry/Exit tab and delete the program stay that was accidentally added for each household member.
#' @inherit data_quality_tables params return
#' @export
dq_duplicate_ees <- function(served_in_date_range, vars, guidance) {
  janitor::get_dupes(served_in_date_range, PersonalID, ProjectID, EntryDate) |>
    dplyr::mutate(
      Issue = "Duplicate Entry Exits",
      Type = "High Priority",
      Guidance = guidance$duplicate_ees
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Future EEs
#' @family Clarity Checks
#' @family DQ: EE Checks
#' @description Users should not be entering a client into a project on a date in the future. If the Entry Date is correct, there is no action needed, but going forward, please be sure that your data entry workflow is correct according to your project type.
#' @inherit data_quality_tables params return
#' @export

dq_future_ees <- function(served_in_date_range, rm_dates, vars, guidance) {
  served_in_date_range |>
    dplyr::filter(EntryDate > DateCreated &
                    (ProjectType %in% c(0, 1, 2, 4, 8, 13) |
                       (
                         ProjectType %in% c(3, 9) &
                           EntryDate >= rm_dates$hc$psh_started_collecting_move_in_date
                       )))  |>
    dplyr::mutate(
      Issue = "Future Entry Date",
      Type = "Warning",
      Guidance = guidance$future_ees
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Future Exits
#' @family Clarity Checks
#' @family DQ: EE Checks
#' @description This client's Exit Date is a date in the future. Please enter the exact date the client left your program. If this client has not yet exited, delete the Exit and then enter the Exit Date once the client is no longer in your program.
#' @inherit data_quality_tables params return
#' @export
dq_future_exits <- function(served_in_date_range, vars, guidance) {
  served_in_date_range |>
    dplyr::filter(ExitDate > lubridate::today()) |>
    dplyr::mutate(
      Issue = "Future Exit Date",
      Type = "Error",
      Guidance = guidance$future_exits
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

# HoHs Entering PH without SPDATs -----------------------------------------

#' @title Find Non-DV HoHs Entering PH, or TH without SPDAT, HoHs in shelter for 8+ days without SPDAT, and SPDAT Created on a Non-HoH
#' @family Clarity Checks
#' @family DQ: SPDAT Checks
#' @description This checks for three warning types:
#' \itemize{
#'   \item{Non-DV HoHs Entering PH or TH without SPDAT}{ Every household (besides those fleeing domestic violence) must have a VI-SPDAT score to aid with prioritization into a Transitional Housing or Permanent Housing (RRH or PSH) project.}
#'   \item{HoHs in shelter for 8+ days without SPDAT}{ Any household who has been in shelter or a Safe Haven for over 8 days should be assessed with the VI-SPDAT so that they can be prioritized for Permanent Housing (RRH or PSH).}
#'   \item{SPDAT Created on a Non-Head-of-Household}{ It is very important to be sure that the VI-SPDAT score goes on the Head of Household of a given program stay because otherwise that score may not pull into any reporting. It is possible a Non Head of Household was a Head of Household in a past program stay, and in that situation, this should not be corrected unless the Head of Household of your program stay is missing their score. To correct this, you would need to completely re-enter the score on the correct client's record.}
#' }
#' @inherit data_quality_tables params return
#' @param Scores HUD Export data frame
#' @param unsh Flag for counting unsheltered clients without SPDATs
#' @export

dq_without_spdats <- function(served_in_date_range, Funder, Scores, rm_dates, vars, unsh = FALSE) {
  va_funded <- Funder_ProjectIDs(Funder)

  no_va <- served_in_date_range |>
    dplyr::anti_join(va_funded, by = "ProjectID")
  ees_with_spdats <- no_va |>
    dplyr::left_join(Scores  |>
                       dplyr::mutate(ScoreAdjusted = dplyr::if_else(is.na(Score), 0, Score),
                                     PersonalID = as.character(PersonalID),
                                     UniqueID = as.character(UniqueID)),
                     by = c("UniqueID", "PersonalID")) |>
    dplyr::ungroup() |>
    dplyr::select(PersonalID,
                  UniqueID,
                  EnrollmentID,
                  RelationshipToHoH,
                  EntryDate,
                  ExitAdjust,
                  ScoreDate,
                  ScoreAdjusted) |>
    dplyr::mutate(ScoreDate = as.Date(ScoreDate)) |>
    dplyr::filter(!is.na(ScoreDate) &
                    ScoreDate + lubridate::days(365) > EntryDate &
                    # score is < 1 yr old
                    ScoreDate <= ExitAdjust) |>  # score is prior to Exit
    dplyr::group_by(EnrollmentID) |>
    dplyr::slice_max(ScoreDate) |>
    dplyr::slice_max(ScoreAdjusted) |>
    dplyr::distinct() |>
    dplyr::ungroup()

  entered_ph_without_spdat <-
    dplyr::anti_join(no_va, ees_with_spdats, by = "EnrollmentID") |>
    dplyr::filter(
      ProjectType %in% c(2, 3, 9, 13) &
        EntryDate > rm_dates$hc$began_requiring_spdats &
        # only looking at 1/1/2019 forward
        RelationshipToHoH == 1 &
        (CurrentlyFleeing != 1 |
           is.na(CurrentlyFleeing) |
           !WhenOccurred %in% c(1:3))
    ) |>
    dplyr::mutate(
      Issue = "Non-DV HoHs Entering PH or TH without SPDAT",
      Type = "Warning",
      Guidance = guidance$ph_without_spdats
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  # HoHs in Shelter without a SPDAT -----------------------------------------
  lh_without_spdat <- served_in_date_range |>
    dplyr::filter(is.na(PHTrack) | PHTrack != "Self Resolve" |
                    ExpectedPHDate < lubridate::today()) |>
    dplyr::anti_join(ees_with_spdats, by = "EnrollmentID") |>
    dplyr::filter(
      ProjectType %in% c(0, 1, 4, 8, 14) &
        VeteranStatus != 1 &
        RelationshipToHoH == 1 &
        EntryDate < lubridate::today() - lubridate::days(8) &
        is.na(ExitDate) &
        EntryDate > rm_dates$hc$began_requiring_spdats
    ) |>
    dplyr::mutate(
      Issue = "HoHs in shelter for 8+ days without SPDAT",
      Type = "Warning",
      Guidance = guidance$lh_without_spdats
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  spdat_on_non_hoh <- ees_with_spdats |>
    dplyr::left_join(
      served_in_date_range,
      by = c(
        "PersonalID",
        "UniqueID",
        "EnrollmentID",
        "RelationshipToHoH",
        "EntryDate",
        "ExitAdjust"
      )
    ) |>
    dplyr::filter(RelationshipToHoH != 1) |>
    dplyr::mutate(
      Issue = "SPDAT Created on a Non-Head-of-Household",
      Type = "Warning",
      Guidance = guidance$spdat_on_non_hoh
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))


  out <- dplyr::bind_rows(spdat_on_non_hoh, lh_without_spdat, entered_ph_without_spdat, va_funded)
  if (unsh && must_sp())
    out <- dplyr::bind_rows(lh_without_spdat, spdat_on_non_hoh)

  out
}

# Income Checks ----
# Thu Sep 16 17:25:10 2021

#' @title Find Conflicting Income yes/no at Entry or Exit
#' @family DQ: Income Checks
#' @family Clarity Checks
#' @family DQ: EE Checks
#' @description If the user answered Yes to Income from any source, then  there should be an income sub-assessment where it indicates which type of income the client is receiving. Similarly if the user answered No, there should not be any income records that say the client is receiving that type of income.
#' @inherit data_quality_tables params return
#' @export


dq_conflicting_income <- function(served_in_date_range, IncomeBenefits, vars, guidance) {
  # Not calculating Conflicting Income Amounts bc they're calculating the TMI from the
  # subs instead of using the field itself. Understandable but that means I would
  # have to pull the TMI data in through RMisc OR we kill TMI altogether. (We
  # decided to kill TMI altogether.)
  smallIncome <- IncomeBenefits |>
    dplyr::select(
      PersonalID,
      EnrollmentID,
      Earned,
      Unemployment,
      SSI,
      SSDI,
      VADisabilityService,
      VADisabilityNonService,
      PrivateDisability,
      WorkersComp,
      TANF,
      GA,
      SocSecRetirement,
      Pension,
      ChildSupport,
      Alimony,
      OtherIncomeSource,
      DataCollectionStage
    )

  smallIncome[is.na(smallIncome)] <- 0

  smallIncome <-
    smallIncome |> dplyr::full_join(IncomeBenefits[c(
      "PersonalID",
      "EnrollmentID",
      "DataCollectionStage",
      "TotalMonthlyIncome",
      "IncomeFromAnySource"
    )],
    by = c("PersonalID",
           "EnrollmentID",
           "DataCollectionStage"))

  income_subs <- served_in_date_range[c("AgeAtEntry",
                                        vars$prep)] |>
    dplyr::left_join(smallIncome, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::mutate(
      IncomeCount =
        Earned +
        Unemployment +
        SSI +
        SSDI +
        VADisabilityService +
        VADisabilityNonService +
        PrivateDisability +
        WorkersComp +
        TANF +
        GA +
        SocSecRetirement +
        Pension +
        ChildSupport +
        Alimony +
        OtherIncomeSource
    )


  conflicting_income_entry <- income_subs |>
    dplyr::filter(DataCollectionStage == 1 &
                    (AgeAtEntry > 17 | is.na(AgeAtEntry)) &
                    ((IncomeFromAnySource == 1 &
                        IncomeCount == 0) |
                       (IncomeFromAnySource == 0 &
                          IncomeCount > 0)
                    )) |>
    dplyr::mutate(Issue = "Conflicting Income yes/no at Entry",
                  Type = "Error",
                  Guidance = guidance$conflicting_income) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  conflicting_income_exit <- income_subs |>
    dplyr::filter(DataCollectionStage == 3 &
                    (AgeAtEntry > 17 | is.na(AgeAtEntry)) &
                    ((IncomeFromAnySource == 1 &
                        IncomeCount == 0) |
                       (IncomeFromAnySource == 0 &
                          IncomeCount > 0)
                    )) |>
    dplyr::mutate(Issue = "Conflicting Income yes/no at Exit",
                  Type = "Error",
                  Guidance = guidance$conflicting_income) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  dplyr::bind_rows(conflicting_income_exit, conflicting_income_exit)

}

#' @title Find Conflicting Income yes/no at Entry or Exit
#' @family DQ: Income Checks
#' @family Clarity Checks
#' @family DQ: EE Checks
#' @description Please enter the data for this item by clicking into the Entry or Exit pencil on the given Client ID on the appropriate program stay.
#' @inherit data_quality_tables params return
#' @export

dq_missing_income <- function(served_in_date_range, IncomeBenefits, vars, guidance) {
  missing_income_entry <- served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      DataCollectionStage,
      TotalMonthlyIncome,
      IncomeFromAnySource
    ) |>
    dplyr::filter(DataCollectionStage == 1 &
                    ProjectName != "Unsheltered Clients - OUTREACH" &
                    (AgeAtEntry > 17 |
                       is.na(AgeAtEntry)) &
                    (IncomeFromAnySource == 99 |
                       is.na(IncomeFromAnySource))) |>
    dplyr::mutate(Issue = "Income Missing at Entry",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  missing_income_exit <- served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      AgeAtEntry,
      DataCollectionStage,
      TotalMonthlyIncome,
      IncomeFromAnySource,
      UserCreating
    ) |>
    dplyr::filter(DataCollectionStage == 3 &
                    (AgeAtEntry > 17 |
                       is.na(AgeAtEntry)) &
                    (IncomeFromAnySource == 99 |
                       is.na(IncomeFromAnySource))) |>
    dplyr::mutate(Issue = "Income Missing at Exit",
                  Type = "Error",
                  Guidance = guidance$missing_at_exit) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  out <- dplyr::bind_rows(missing_income_entry, missing_income_exit) |>
    dplyr::distinct(PersonalID, .keep_all = TRUE)
  return(out)
}

# Overlapping Enrollment/Move In Dates ------------------------------------



#' @title Find Overlapping Project Stays on the Same Day
#' @family Clarity Checks
#' @family DQ: Overlapping Enrollment/Move-In Dates
#' @param unsh Flag to check unsheltered clients
#' @inherit dq_overlaps params return description
#' @export
overlaps_same_day <- function(served_in_date_range, vars, guidance,
                              data_types, unsh = FALSE) {
  out <- served_in_date_range |>
    dplyr::filter((ProjectType == 13 & MoveInDateAdjust == ExitDate) |
                    ProjectType != 13) |>
    dplyr::filter(ProjectType != data_types$Project$ProjectType$ap) |>
    dplyr::select(dplyr::all_of(vars$prep), ExitAdjust) |>
    dplyr::mutate(
      EntryAdjust = dplyr::case_when(
        #for PSH and RRH, EntryAdjust = MoveInDate
        ProjectType %in% data_types$Project$ProjectType$lh_hp |
          ProjectName == "Unsheltered Clients - OUTREACH" ~ EntryDate,
        ProjectType %in% data_types$Project$ProjectType$ph &
          !is.na(MoveInDateAdjust) ~ MoveInDateAdjust,
        ProjectType %in% data_types$Project$ProjectType$ph &
          is.na(MoveInDateAdjust) ~ EntryDate
      ),
      LiterallyInProject = dplyr::case_when(
        ProjectType %in% c(3, 9) ~ lubridate::interval(MoveInDateAdjust, ExitAdjust),
        ProjectType %in% data_types$Project$ProjectType$lh_hp_so ~ lubridate::interval(EntryAdjust, ExitAdjust)
      ),
      Issue = "Overlapping Project Stays",
      Type = "High Priority",
      Guidance = guidance$project_stays
    ) |>
    dplyr::filter((!is.na(LiterallyInProject) & ProjectType != 13) |
                    ProjectType == 13) |>
    janitor::get_dupes(PersonalID) |>
    dplyr::group_by(PersonalID) |>
    dplyr::arrange(PersonalID, EntryAdjust) |>
    dplyr::mutate(
      PreviousEntryAdjust = dplyr::lag(EntryAdjust),
      PreviousExitAdjust = dplyr::lag(ExitAdjust),
      PreviousProject = dplyr::lag(ProjectName)
    ) |>
    dplyr::filter(ExitDate > PreviousEntryAdjust &
                    ExitDate < PreviousExitAdjust) |>
    dplyr::ungroup()

  out <- dplyr::select(out, dplyr::all_of(c(vars$we_want, "PreviousProject")))
  return(out)
}

sum_enroll_overlap <- function(PersonalID, EnrollmentID, Stay) {
  PersonalID <- unique(PersonalID)
  .movein <- FALSE
  x <- data.frame(EnrollmentID, Stay)
  out <- character()

  while (nrow(x) > 1) {
    ol_eids <- NULL
    .x <- x$Stay[1]
    .y <- x$EnrollmentID[1]
    x <- dplyr::filter(x, EnrollmentID != .y)
    ol <- lubridate::int_overlaps(.x, x$Stay)

    if (any(ol)) {
      ol_eids <- x$EnrollmentID[ol]
      # Create text hyperlinks
      out <- paste0(out, purrr::when(length(out), . != 0 ~ "\n", character()), paste0(clarity.looker::make_link(PersonalID, .y, type = "enrollment"), " overlaps: ", paste0(clarity.looker::make_link(PersonalID, ol_eids, type = "enrollment"), collapse = ", ")))
      x <- dplyr::filter(x, !EnrollmentID %in% ol_eids)
    }
  }
  out
}


#' @title Find Overlapping Project Stays for Selected Project Types
#' @param p_types \code{(numeric)} Project Types for which to check for overlaps between.
#' @family Clarity Checks
#' @family Unsheltered Checks
#' @family ServicePoint Checks
#' @family DQ: Overlapping Enrollment/Move-In Dates
#' @param unsh Flag for if clients are unsheltered or not
#' @inherit dq_overlaps params return description
#' @export
overlaps <- function(served_in_date_range, p_types = data_types$Project$ProjectType$ph, vars, guidance, unsh = FALSE) {
  project_types <- list()
  project_types$ph <- c(3, 9, 13)

  out <- served_in_date_range |>
    dplyr::filter(ProjectType %in% p_types) |>
    dplyr::select(dplyr::all_of(vars$prep), ExitAdjust, EnrollmentID) |>
    janitor::get_dupes(PersonalID) |>
    dplyr::mutate(
      Stay = lubridate::interval(EntryDate, ExitAdjust - lubridate::days(1))
    ) |>
    dplyr::group_by(PersonalID) |>
    dplyr::arrange(EntryDate) |>
    dplyr::summarise(Overlaps = sum_enroll_overlap(PersonalID, EnrollmentID, Stay), .groups = "drop") |>
    dplyr::filter(purrr::map_int(Overlaps, length) != 0) |>
    dplyr::mutate(
      Issue = "Overlapping Project Stay",
      Type = "High Priority",
      Guidance = eval(parse(text = guidance$project_stays_eval))
    ) |>
    clarity.looker::make_linked_df(Overlaps, unlink = TRUE, new_ID = EnrollmentID) |>
    dplyr::left_join(
      dplyr::select(served_in_date_range, EnrollmentID, ExitDate, EntryDate, ProjectID, ProjectName, MoveInDateAdjust)
      , by = "EnrollmentID")

  return(out)
}


#' @title Find Overlapping Project Stays
#' @family Clarity Checks
#' @family ServicePoint Checks
#' @family DQ: Overlapping Enrollment/Move-In Dates
#' @description This function returns a data frame of clients that have overlapping project stays
#' @inherit data_quality_tables params return
#' @export

dq_overlaps <- function(served_in_date_range, vars, guidance, data_types) {
  p_types = data_types$Project$ProjectType$lh_hp
  project_types <- list()
  project_types$ph <- c(3, 9, 13)
  overlap_staging <- served_in_date_range |>
    dplyr::select(!!vars$prep, ExitAdjust, EnrollmentID) |>
    dplyr::filter(EntryDate != ExitAdjust &
                    ((
                      ProjectType %in% c(data_types$Project$ProjectType$ph, 10) &
                        !is.na(MoveInDateAdjust)
                    ) |
                      ProjectType %in% c(data_types$Project$ProjectType$lh, 0)
                    )) |>
    dplyr::mutate(
      EnrollmentStart = dplyr::case_when(
        ProjectType %in% c(data_types$Project$ProjectType$lh) ~ EntryDate,
        ProjectType %in% c(data_types$Project$ProjectType$ph) ~ MoveInDateAdjust,
        TRUE ~ EntryDate
      ),
      EnrollmentEnd =  as.Date(ExitAdjust)) |>
    dplyr::select(PersonalID, EnrollmentID, ProjectType, EnrollmentStart, EnrollmentEnd)

  overlaps_enroll <- overlap_staging |>
    # sort enrollments for each person
    dplyr::group_by(PersonalID) |>
    dplyr::arrange(EnrollmentStart, EnrollmentEnd) |>
    dplyr::mutate(
      # pull in previous enrollment into current enrollment record so we can compare intervals
      PreviousEnrollmentID = dplyr::lag(EnrollmentID)) |>
    dplyr::ungroup() |>
    dplyr::filter(!is.na(PreviousEnrollmentID))  |>
    dplyr::left_join(overlap_staging |>
                       dplyr::select("PreviousEnrollmentID" = EnrollmentID,
                                     "PreviousProjectType" = ProjectType,
                                     "PreviousEnrollmentStart" = EnrollmentStart,
                                     "PreviousEnrollmentEnd" = EnrollmentEnd
                       ),
                     by = c("PreviousEnrollmentID")) |>
    dplyr::filter(PreviousEnrollmentID != EnrollmentID &
                    !(
                      (ProjectType == data_types$Project$ProjectType$psh &
                         PreviousProjectType %in% c(data_types$Project$ProjectType$rrh)) |
                        (PreviousProjectType == data_types$Project$ProjectType$psh &
                           ProjectType %in% c(data_types$Project$ProjectType$rrh))
                    )) |>
    # flag overlaps
    dplyr::mutate(
      EnrollmentPeriod = lubridate::interval(EnrollmentStart, EnrollmentEnd),
      PreviousEnrollmentPeriod =
        lubridate::interval(PreviousEnrollmentStart, PreviousEnrollmentEnd),
      IsOverlap = lubridate::int_overlaps(EnrollmentPeriod, PreviousEnrollmentPeriod) &
        EnrollmentStart != PreviousEnrollmentEnd) |>
    dplyr::filter(IsOverlap == TRUE)  |>
    dplyr::group_by(PersonalID) |>
    dplyr::mutate(NumOverlaps = sum(IsOverlap, na.rm = TRUE)) |>
    dplyr::ungroup() |>
    # keep overlaps
    dplyr::filter(NumOverlaps > 0) |>
    # label issue types
    dplyr::mutate(Issue = "Overlapping Project Stay & Move-In",
                  Type = "High Priority",
                  Guidance = eval(parse(text = guidance$project_stays_eval)))

  oldnames <- c("PersonalID","EnrollmentID","ProjectType","EnrollmentStart",
                "EnrollmentEnd","PreviousEnrollmentID","PreviousProjectType",
                "PreviousEnrollmentStart","PreviousEnrollmentEnd","EnrollmentPeriod",
                "PreviousEnrollmentPeriod","IsOverlap","NumOverlaps","Issue","Type","Guidance")

  newnames <- c("PersonalID","PreviousEnrollmentID","PreviousProjectType","PreviousEnrollmentStart",
                "PreviousEnrollmentEnd","EnrollmentID","ProjectType",
                "EnrollmentStart","EnrollmentEnd","PreviousEnrollmentPeriod",
                "EnrollmentPeriod","IsOverlap","NumOverlaps","Issue","Type","Guidance")

  overlaps_prev_enroll <- overlaps_enroll |>
    dplyr::rename_at(dplyr::vars(oldnames), ~ newnames) |>
    dplyr::select(oldnames)

  out <- rbind(overlaps_enroll, overlaps_prev_enroll) |>
    dplyr::mutate(Overlaps = paste0(clarity.looker::make_link(PersonalID, EnrollmentID, type = "enrollment"),
                                    " overlaps: ",
                                    paste0(clarity.looker::make_link(PersonalID, PreviousEnrollmentID, type = "enrollment")))) |>
    dplyr::select(EnrollmentID, Overlaps, PreviousEnrollmentID, Issue, Type, Guidance) |>
    unique() |>
    dplyr::left_join(served_in_date_range |>
                       dplyr::select(!!vars$prep, EnrollmentID), by = "EnrollmentID") |>
    dplyr::select(vars$prep, "Issue", "Type", "Guidance")

  out <- clarity.looker::make_linked_df(out, UniqueID)

  return(out)
}
# Missing Health Ins ------------------------------------------------------
#' @title Find Missing Health Insurance at Entry
#' @family Clarity Checks
#' @family DQ: Health Insurance Checks
#' @description This data element is required to be collected at project Entry or Exit. Please click into the client's Entry/Exit pencil to save this data to HMIS.
#' @inherit data_quality_tables params return
#' @export


dq_missing_hi_entry <- function(served_in_date_range,  IncomeBenefits, vars, guidance) {
  served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(dplyr::all_of(vars$prep),
                  AgeAtEntry,
                  DataCollectionStage,
                  InsuranceFromAnySource) |>
    dplyr::filter(DataCollectionStage == 1 &
                    ProjectName != "Unsheltered Clients - OUTREACH" &
                    (InsuranceFromAnySource == 99 |
                       is.na(InsuranceFromAnySource))) |>
    dplyr::mutate(Issue = "Health Insurance Missing at Entry",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Missing Health Insurance at Exit
#' @family Clarity Checks
#' @family DQ: Health Insurance Checks
#' @description This data element is required to be collected at project Entry or Exit. Please click into the client's Entry/Exit pencil to save this data to HMIS.
#' @inherit data_quality_tables params return
#' @export
dq_missing_hi_exit <- function(served_in_date_range,  IncomeBenefits, vars, guidance) {
  served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(dplyr::all_of(vars$prep),
                  DataCollectionStage,
                  InsuranceFromAnySource) |>
    dplyr::filter(DataCollectionStage == 3 &
                    ProjectName != "Unsheltered Clients - OUTREACH" &
                    (InsuranceFromAnySource == 99 |
                       is.na(InsuranceFromAnySource))) |>
    dplyr::mutate(Issue = "Health Insurance Missing at Exit",
                  Type = "Error",
                  Guidance = guidance$missing_at_exit) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Conflicting Health Insurance at Entry/Exit
#' @family Clarity Checks
#' @family DQ: Health Insurance Checks
#' @description If the user answered 'Yes' to 'Covered by Health Insurance?', then there should be a Health Insurance subassessment where it indicates which type of health insurance the client is receiving. Similarly if the user answered 'No', there should not be any Health Insurance records that say the client is receiving that type of Health Insurance.
#' @inherit data_quality_tables params return
#' @export


dq_conflicting_hi_ee <- function(served_in_date_range,  IncomeBenefits, vars, guidance) {
  hi_subs <-
    served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(
      dplyr::all_of(vars$prep),
      DataCollectionStage,
      InsuranceFromAnySource,
      Medicaid,
      Medicare,
      SCHIP,
      VHAServices,
      EmployerProvided,
      COBRA,
      PrivatePay,
      StateHealthIns,
      IndianHealthServices,
      OtherInsurance,
      ADAP,
      UserCreating
    ) |>
    dplyr::mutate(
      SourceCount = Medicaid + SCHIP + VHAServices + EmployerProvided +
        COBRA + PrivatePay + StateHealthIns + IndianHealthServices +
        OtherInsurance + Medicare
    )

  conflicting_hi_entry <- hi_subs |>
    dplyr::filter(DataCollectionStage == 1 &
                    ProjectName != "Unsheltered Clients - OUTREACH" &
                    ((InsuranceFromAnySource == 1 &
                        SourceCount == 0) |
                       (InsuranceFromAnySource == 0 &
                          SourceCount > 0)
                    )) |>
    dplyr::mutate(Issue = "Conflicting Health Insurance yes/no at Entry",
                  Type = "Error",
                  Guidance = guidance$conflicting_hi) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  conflicting_hi_exit <- hi_subs |>
    dplyr::filter(DataCollectionStage == 3 &
                    ProjectName != "Unsheltered Clients - OUTREACH" &
                    ((InsuranceFromAnySource == 1 &
                        SourceCount == 0) |
                       (InsuranceFromAnySource == 0 &
                          SourceCount > 0)
                    )) |>
    dplyr::mutate(
      Issue = "Conflicting Health Insurance yes/no at Exit",
      Type = "Error",
      Guidance = guidance$conflicting_hi
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  dplyr::bind_rows(conflicting_hi_entry, conflicting_hi_exit)
}

#' @title Find Missing Non-Cash Benefits (NCBS) at Entry/Exit
#' @family Clarity Checks
#' @family DQ: Non-Cash Benefit Checks
#' @description This data element is required to be collected at project Entry or Exit. Please click into the client's Entry/Exit pencil to save this data to HMIS.
#' @inherit data_quality_tables params return
#' @export

dq_missing_ncbs <- function(served_in_date_range, IncomeBenefits, vars, guidance) {
  missing_ncbs_entry <- served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(AgeAtEntry,
                  dplyr::all_of(vars$prep),
                  DataCollectionStage,
                  BenefitsFromAnySource) |>
    dplyr::filter(
      DataCollectionStage == 1 &
        (AgeAtEntry > 17 |
           is.na(AgeAtEntry)) &
        (BenefitsFromAnySource == 99 |
           is.na(BenefitsFromAnySource))
    ) |>
    dplyr::mutate(Issue = "Non-cash Benefits Missing at Entry",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  missing_ncbs_exit <- served_in_date_range |>
    dplyr::left_join(IncomeBenefits, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(AgeAtEntry,
                  dplyr::all_of(vars$prep),
                  DataCollectionStage,
                  BenefitsFromAnySource) |>
    dplyr::filter(
      DataCollectionStage == 3 &
        (AgeAtEntry > 17 |
           is.na(AgeAtEntry)) &
        (BenefitsFromAnySource == 99 |
           is.na(BenefitsFromAnySource))
    ) |>
    dplyr::mutate(Issue = "Non-cash Benefits Missing at Exit",
                  Type = "Error",
                  Guidance = guidance$missing_at_exit) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  out <- dplyr::bind_rows(missing_ncbs_exit, missing_ncbs_entry) |>
    dplyr::distinct(PersonalID, .keep_all = TRUE)
  return(out)
}

#' @title Find Conflicting or Unlikely Non-Cash Benefits (NCBS) at Entry/Exit
#' @family DQ: Non-Cash Benefit Checks
#' @family Clarity Checks
#' @description
#' \itemize{
#'   \item{Conflicting NCBs}{  If the user answered 'Yes' to 'Non-cash benefits from any source', then there should be a Non-cash benefits subassessment where it indicates which type of income the client is receiving. Similarly if the user answered 'No', then there should not be any non-cash records that say the client is receiving that type of benefit}
#'   \item{Unlikeley NCBs}{ This client has every single Non-Cash Benefit, according to HMIS, which is highly unlikely. Please correct (unless it's actually true).}
#' }
#' @inherit data_quality_tables params return
#' @export

dq_conflicting_unlikely_ncbs <- function(served_in_date_range, IncomeBenefits, vars, guidance) {
  ncb_subs <- IncomeBenefits |>
    dplyr::select(
      PersonalID,
      EnrollmentID,
      DataCollectionStage,
      SNAP,
      WIC,
      TANFChildCare,
      TANFTransportation,
      OtherTANF,
      OtherBenefitsSource
    )

  ncb_subs[is.na(ncb_subs)] <- 0

  ncb_subs <- ncb_subs |>
    dplyr::full_join(IncomeBenefits[c("PersonalID",
                                      "EnrollmentID",
                                      "DataCollectionStage",
                                      "BenefitsFromAnySource")],
                     by = c("PersonalID",
                            "EnrollmentID",
                            "DataCollectionStage"))

  ncb_subs <- served_in_date_range |>
    dplyr::filter(ProjectName != "Unsheltered Clients - OUTREACH") |>
    dplyr::left_join(ncb_subs, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(
      PersonalID,
      EnrollmentID,
      HouseholdID,
      AgeAtEntry,
      ProjectName,
      EntryDate,
      MoveInDateAdjust,
      ExitDate,
      ProjectType,
      DataCollectionStage,
      BenefitsFromAnySource,
      SNAP,
      WIC,
      TANFChildCare,
      TANFTransportation,
      OtherTANF,
      OtherBenefitsSource,
      UserCreating
    ) |>
    dplyr::mutate(
      BenefitCount = SNAP + WIC + TANFChildCare + TANFTransportation +
        OtherTANF + OtherBenefitsSource
    ) |>
    dplyr::select(PersonalID,
                  EnrollmentID,
                  DataCollectionStage,
                  BenefitsFromAnySource,
                  BenefitCount) |>
    unique()

  unlikely_ncbs_entry <- served_in_date_range |>
    dplyr::left_join(ncb_subs, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(
      AgeAtEntry,
      dplyr::all_of(vars$prep),
      DataCollectionStage,
      BenefitsFromAnySource,
      BenefitCount
    ) |>
    dplyr::filter(DataCollectionStage == 1 &
                    (AgeAtEntry > 17 | is.na(AgeAtEntry)) &
                    (BenefitCount == 6)) |>
    dplyr::mutate(Issue = "Client has ALL SIX Non-cash Benefits at Entry",
                  Type = "Warning",
                  Guidance = guidance$unlikely_ncbs) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  conflicting_ncbs_entry <- served_in_date_range |>
    dplyr::left_join(ncb_subs, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(AgeAtEntry,
                  dplyr::all_of(vars$prep),
                  DataCollectionStage,
                  BenefitsFromAnySource,
                  BenefitCount) |>
    dplyr::filter(DataCollectionStage == 1 &
                    (AgeAtEntry > 17 | is.na(AgeAtEntry)) &
                    ((BenefitsFromAnySource == 1 &
                        BenefitCount == 0) |
                       (BenefitsFromAnySource == 0 &
                          BenefitCount > 0)
                    )) |>
    dplyr::mutate(Issue = "Conflicting Non-cash Benefits yes/no at Entry",
                  Type = "Error",
                  Guidance = guidance$conflicting_ncbs) |>
    dplyr::select(dplyr::all_of(vars$we_want))


  conflicting_ncbs_exit <- served_in_date_range |>
    dplyr::left_join(ncb_subs, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::select(
      AgeAtEntry,
      dplyr::all_of(vars$prep),
      DataCollectionStage,
      BenefitsFromAnySource,
      BenefitCount
    ) |>
    dplyr::filter(DataCollectionStage == 3 &
                    (AgeAtEntry > 17 | is.na(AgeAtEntry)) &
                    ((BenefitsFromAnySource == 1 &
                        BenefitCount == 0) |
                       (BenefitsFromAnySource == 0 &
                          BenefitCount > 0)
                    )) |>
    dplyr::mutate(Issue = "Conflicting Non-cash Benefits yes/no at Exit",
                  Type = "Error",
                  Guidance = guidance$conflicting_ncbs) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  dplyr::bind_rows(unlikely_ncbs_entry, conflicting_ncbs_exit, conflicting_ncbs_entry)

}

#' @title Find SSI/SSDI but no Disability
#' @family Clarity Checks
#' @description `r guidance$check_disability_ssi`
#' @inherit data_quality_tables params return
#' @export

dq_check_disability_ssi <- function(served_in_date_range, IncomeBenefits, vars, guidance) {
  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  EnrollmentID,
                  AgeAtEntry,
                  DisablingCondition) |>
    dplyr::left_join(IncomeBenefits |>
                       dplyr::select(EnrollmentID, PersonalID, SSI, SSDI), by = c("EnrollmentID", "PersonalID")) |>
    dplyr::mutate(SSI = dplyr::if_else(is.na(SSI), 0L, SSI),
                  SSDI = dplyr::if_else(is.na(SSDI), 0L, SSDI)) |>
    dplyr::filter(SSI + SSDI > 0 &
                    DisablingCondition == 0 & AgeAtEntry > 17) |>
    dplyr::select(-DisablingCondition, -SSI, -SSDI, -AgeAtEntry) |>
    unique() |>
    dplyr::mutate(
      Issue = "Client with No Disability Receiving SSI/SSDI (could be ok)",
      Type = "Warning",
      Guidance = guidance$check_disability_ssi
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Services on Household Members
#' @family Clarity Checks
#' @description `r guidance$services_on_non_hoh`
#' @family DQ: Household Checks
#' @inherit data_quality_tables params return
#' @param Services_enroll_extras Custom Services and Enrollment data frame
#' @export

dq_services_on_non_hoh <- function(served_in_date_range, Services_enroll_extras, vars, rm_dates, guidance) {
  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  EnrollmentID,
                  RelationshipToHoH,
                  GrantType) |>
    dplyr::filter(
      RelationshipToHoH != 1 &
        EntryDate >= rm_dates$hc$no_more_svcs_on_hh_members &
        (GrantType != "SSVF" | is.na(GrantType))
    ) |>
    dplyr::semi_join(Services_enroll_extras, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::mutate(Issue = "Service Transaction on a Non Head of Household",
                  Type = "Warning",
                  Guidance = guidance$services_on_non_hoh) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Services on Household Members in SSVF
#' @family Clarity Checks
#' @description `r guidance$services_on_non_hoh`
#' @family DQ: Household Checks
#' @inherit data_quality_tables params return
#' @param Services_enroll_extras Customs Services and Enrollment data frame
#' @export

dq_services_on_hh_members_ssvf <- function(served_in_date_range, Services_enroll_extras, vars, guidance) {
  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  EnrollmentID,
                  RelationshipToHoH,
                  GrantType) |>
    dplyr::filter(RelationshipToHoH != 1 &
                    GrantType == "SSVF") |>
    dplyr::semi_join(Services_enroll_extras, by = c("PersonalID", "EnrollmentID")) |>
    dplyr::mutate(Issue = "Service Transaction on a Non Head of Household (SSVF)",
                  Type = "Error",
                  Guidance = guidance$services_on_non_hoh) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Old Outstanding Referrals
#' @family Clarity Checks
#' @family DQ: Referral Checks
#' @inherit data_quality_tables params return
#' @export


dq_referrals_outstanding <- function(served_in_date_range, Referrals, vars) {
  # Old Outstanding Referrals -----------------------------------------------
  # CW says ProviderCreating should work instead of Referred-From Provider
  # Using ProviderCreating instead. Either way, I feel this should go in the
  # Provider Dashboard, not the Data Quality report.
  Referrals <- Referrals |>
    dplyr::mutate_at(dplyr::vars("PersonalID", "UniqueID"), as.character)
  served_in_date_range |>
    dplyr::semi_join(Referrals,
                     by = c("PersonalID", "UniqueID")) |>
    dplyr::left_join(Referrals, by = c("PersonalID", "UniqueID")) |>
    dplyr::select(dplyr::all_of(vars$prep),
                  ReferringProjectID,
                  ReferralDaysElapsed,
                  ReferringProjectName,
                  DaysInQueue,
                  EnrollmentID) |>
    dplyr::filter(ReferralDaysElapsed %|% DaysInQueue > 14) |>
    dplyr::mutate(
      ProjectName = ReferringProjectName,
      ProjectID = ReferringProjectID,
      Issue = "Old Outstanding Referral",
      Type = "Warning",
      Guidance = "Referrals should be closed in about 2 weeks. Please be sure you are following up with any referrals and helping the client to find permanent housing. Once a Referral is made, the receiving agency should be saving the 'Referral Outcome' once it is known. If you have Referrals that are legitimately still open after 2 weeks because there is a lot of follow up going on, no action is needed since the HMIS data is accurate."
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Find Referrals on Household Members in SSVF
#' @family Clarity Checks
#' @family DQ: Household Checks
#' @inherit data_quality_tables params return
#' @export

dq_referrals_on_hh_members_ssvf <- function(served_in_date_range, Referrals, vars, guidance) {
  SSVF_Referrals <- Referrals |>
    dplyr::filter(stringr::str_detect(ReferredProjectName, "SSVF")) |>
    dplyr::mutate(PersonalID = as.character(PersonalID))
  served_in_date_range |>
    dplyr::select(dplyr::all_of(vars$prep),
                  RelationshipToHoH,
                  EnrollmentID,
                  GrantType) |>
    dplyr::filter(RelationshipToHoH != 1 &
                    GrantType == "SSVF") |>
    dplyr::semi_join(SSVF_Referrals, by = c("PersonalID")) |>
    dplyr::mutate(Issue = "Referral on a Non Head of Household (SSVF)",
                  Type = "Error",
                  Guidance = guidance$referral_ssvf_on_non_hoh) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


# SSVF --------------------------------------------------------------------

#' @title Create `ssvf_served_in_date_range` for Clients receiving grants from SSVF
#'
#' @inherit served_in_date_range params return
#' @param served_in_date_range \code{(function)} A function that filters clients based on a date range. See `served_in_date_range` for more details.
#' @export

ssvf_served_in_date_range <- function(Enrollment_extra_Client_Exit_HH_CL_AaE, served_in_date_range, Client) {
  Enrollment_extra_Client_Exit_HH_CL_AaE |>
    dplyr::select(dplyr::all_of(
      c(
        "EnrollmentID",
        "EntryAdjust",
        "EntryDate",
        "ExitDate",
        "HouseholdID",
        "HPScreeningScore",
        "MoveInDateAdjust",
        "PercentAMI",
        "PersonalID",
        "ProjectID",
        "ProjectName",
        "ProjectType",
        "RelationshipToHoH",
        "ThresholdScore",
        "UniqueID",
        "UserCreating",
        "VAMCStation"
      )
    )) |>
    dplyr::right_join(
      served_in_date_range |>
        dplyr::filter(GrantType == "SSVF") |>
        dplyr::select(PersonalID, EnrollmentID, HouseholdID, ProjectRegion),
      by = c("PersonalID", "EnrollmentID", "HouseholdID")
    ) |>
    dplyr::left_join(
      Client |>
        dplyr::select(dplyr::all_of(
          c("PersonalID",
            "VeteranStatus",
            "YearEnteredService",
            "YearSeparated",
            "WorldWarII",
            "KoreanWar",
            "VietnamWar",
            "DesertStorm",
            "AfghanistanOEF",
            "IraqOIF",
            "IraqOND",
            "OtherTheater",
            "MilitaryBranch",
            "DischargeStatus")
        )),
      by = "PersonalID"
    )
}

#' @title Data quality report on Veteran Status
#' @family Clarity Checks
#' @family DQ: Missing UDEs

#' @inherit data_quality_tables params return

dq_veteran <- function(served_in_date_range, guidance = NULL, vars = NULL) {
  adult = rlang::expr((AgeAtEntry >= 18 | is.na(AgeAtEntry)))
  vet_expr <- rlang::exprs(
    missing = !!adult &
      VeteranStatus == 99,
    dkr = !!adult &
      VeteranStatus %in% c(8, 9),
    check = !!adult &
      RelationshipToHoH == 1 &
      VeteranStatus == 0 &
      Destination %in% c(19, 28),
    .named = TRUE
  )
  out <- served_in_date_range |>
    dplyr::group_by(PersonalID) |>
    dplyr::mutate(AgeAtEntry = valid_max(AgeAtEntry)) |>
    dplyr::ungroup() |>
    dplyr::filter((!!vet_expr$missing) | (!!vet_expr$dkr) | (!!vet_expr$check)) |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        !!vet_expr$missing ~ "Missing Veteran Status",
        !!vet_expr$dkr ~ "Don't Know/Prefers Not to Answer Veteran Status",
        !!vet_expr$check ~ "Check Veteran Status for Accuracy"
      ),
      Type = dplyr::case_when(
        Issue == "Missing Veteran Status" ~ "Error",
        Issue %in% c(
          "Don't Know/Prefers Not to Answer Veteran Status",
          "Check Veteran Status for Accuracy"
        ) ~ "Warning"
      ),
      Guidance = dplyr::case_when(
        Issue == "Check Veteran Status for Accuracy" ~ guidance$check_vet_status,
        Issue == "Missing Veteran Status" ~ guidance$missing_pii,
        Issue == "Don't Know/Prefers Not to Answer Veteran Status" ~ guidance$dkr_data)
    ) |>
    dplyr::select(dplyr::all_of(vars$we_want))
  return(out)
}


#' @title Check for missing Year Entered on clients who are Veterans
#'
#' @param ssvf_served_in_date_range See \code{ssvf_served_in_date_range}
#' @inherit data_quality_tables params return
#' @family DQ: EE Checks
#' @family DQ: SSVF Checks
#' @export

dq_veteran_missing_year_entered <- function(ssvf_served_in_date_range, vars, guidance) {
  ssvf_served_in_date_range |>
    dplyr::filter(VeteranStatus == 1) |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        is.na(YearEnteredService) ~ "Missing Year Entered Service",
        YearEnteredService > lubridate::year(lubridate::today()) ~ "Incorrect Year Entered Service"),
      Type = "Error",
      Guidance = guidance$missing_at_entry
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


#' @title Check for missing Year Separated on clients who are Veterans
#'
#' @inherit dq_veteran_missing_year_entered params return
#' @family DQ: SSVF Checks
#' @family DQ: EE Checks
#' @export

dq_veteran_missing_year_separated <- function(ssvf_served_in_date_range, vars, guidance) {
  ssvf_served_in_date_range |>
    dplyr::filter(VeteranStatus == 1) |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        is.na(YearSeparated) ~ "Missing Year Separated",
        YearSeparated > lubridate::year(lubridate::today()) ~ "Incorrect Year Separated"),
      Type = "Error",
      Guidance = guidance$missing_at_entry
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  veteran_missing_wars <- ssvf_served_in_date_range |>
    dplyr::filter(
      VeteranStatus == 1 &
        (
          is.na(WorldWarII) | WorldWarII == 99 |
            is.na(KoreanWar) | KoreanWar == 99 |
            is.na(VietnamWar) | VietnamWar == 99 |
            is.na(DesertStorm) | DesertStorm == 99 |
            is.na(AfghanistanOEF) | AfghanistanOEF == 99 |
            is.na(IraqOIF) | IraqOIF == 99 |
            is.na(IraqOND) | IraqOND == 99 |
            is.na(OtherTheater) |
            OtherTheater == 99
        )
    ) |>
    dplyr::mutate(Issue = "Missing War(s)",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  return(veteran_missing_wars)
}


#' @title Check for missing Branch on clients who are Veterans
#'
#' @inherit dq_veteran_missing_year_entered params return
#' @family DQ: SSVF Checks
#' @family DQ: EE Checks
#' @export

dq_veteran_missing_branch <- function(ssvf_served_in_date_range, guidance, vars) {
  ssvf_served_in_date_range |>
    dplyr::filter(VeteranStatus == 1 &
                    is.na(MilitaryBranch)) |>
    dplyr::mutate(Issue = "Missing Military Branch",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Check for missing Discharge Status on clients who are Veterans
#'
#' @inherit dq_veteran_missing_year_entered params return
#' @family DQ: SSVF Checks
#' @family DQ: EE Checks
#' @export

dq_veteran_missing_discharge_status <- function(ssvf_served_in_date_range, vars, guidance) {
  ssvf_served_in_date_range |>
    dplyr::filter(VeteranStatus == 1 & is.na(DischargeStatus)) |>
    dplyr::mutate(Issue = "Missing Discharge Status",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}

#' @title Check for Dont Know/Prefers Not to Answer Wars/Branch/Discharge on clients who are Veterans
#'
#' @inherit dq_veteran_missing_year_entered params return
#' @family DQ: SSVF Checks
#' @family DQ: EE Checks
#' @export

dq_dkr_client_veteran_info <- function(ssvf_served_in_date_range, vars, guidance) {
  ssvf_served_in_date_range |>
    dplyr::filter(VeteranStatus == 1) |>
    dplyr::mutate(
      Issue = dplyr::case_when(
        WorldWarII %in% c(8, 9) |
          KoreanWar %in% c(8, 9) |
          VietnamWar %in% c(8, 9) |
          DesertStorm  %in% c(8, 9) |
          AfghanistanOEF %in% c(8, 9) |
          IraqOIF %in% c(8, 9) |
          IraqOND %in% c(8, 9) |
          OtherTheater  %in% c(8, 9)  ~ "Don't Know/Prefers Not to Answer War(s)",
        MilitaryBranch %in% c(8, 9) ~ "Missing Military Branch",
        DischargeStatus %in% c(8, 9) ~ "Missing Discharge Status"
      ),
      Type = "Warning",
      Guidance = guidance$dkr_data
    ) |>
    dplyr::filter(!is.na(Issue)) |>
    dplyr::select(dplyr::all_of(vars$we_want))
}


#' @title Check for missing Percent AMI on clients who are Veterans
#'
#' @inherit dq_veteran_missing_year_entered params return
#' @family DQ: SSVF Checks
#' @family DQ: EE Checks
#' @export

dq_ssvf_missing_percent_ami <- function(ssvf_served_in_date_range, vars, guidance) {
  ssvf_served_in_date_range |>
    dplyr::filter(RelationshipToHoH == 1 &
                    is.na(PercentAMI)) |>
    dplyr::mutate(Issue = "Missing Percent AMI",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  ssvf_missing_vamc <- ssvf_served_in_date_range |>
    dplyr::filter(RelationshipToHoH == 1 &
                    is.na(VAMCStation)) |>
    dplyr::mutate(Issue = "Missing VAMC Station Number",
                  Type = "Error",
                  Guidance = guidance$missing_at_entry) |>
    dplyr::select(dplyr::all_of(vars$we_want))

  return(ssvf_missing_vamc)
}

# #' Find enrollments without a referral
# #'
# #' @param served_in_date_range
# #' @param vars
# #' @param guidance
# #' @param app_env
# #'
# #' @return
# #' @export
# #'
# #' @examples
# dq_missing_referral <- function(served_in_date_range, Referrals, vars, guidance = NULL, app_env = get_app_env(e = rlang::caller_env())) {
#   if (is_app_env(app_env))
#     app_env$set_parent(missing_fmls())
#
# }

# AP No Recent Referrals --------------------------------------------------

dqu_aps <- function(Project, Referrals, data_APs = TRUE) {
  co_APs <- Project |>
    dplyr::filter(ProjectType == 14) |> # not incl Mah CE
    dplyr::select(
      ProjectID,
      OperatingStartDate,
      OperatingEndDate,
      ProjectName,
      HMISParticipationType,
      ProjectCounty
    ) |>
    dplyr::distinct()
  participating <- dplyr::filter(co_APs, HMISParticipationType == 1) |>
    unique() |>
    dplyr::pull(ProjectID)
  referring <- unique(Referrals$R_ReferringProjectID)

  aps_no_referrals <- setdiff(participating, referring)
  aps_w_referrals <- intersect(participating, referring)


  if (data_APs) {
    l <- c(no = length(aps_no_referrals), w = length(aps_w_referrals))
    out <- tibble::tibble(
      category = c("Not Referring", "Referring"),
      count = l,
      providertype = rep("Access Points"),
      total = rep(sum(l)),
      stringsAsFactors = FALSE
    ) |>
      dplyr::mutate(percent = count / total,
                    prettypercent = scales::percent(count / total))
  } else {
    out <- co_APs |>
      dplyr::filter(ProjectID %in% aps_no_referrals) |>
      dplyr::select(ProjectID, ProjectName, OperatingEndDate)

  }

  return(out)
}



read_roxygen <- function(file = file.path("R","04_DataQuality_utils.R"), tag = "family") {
  readLines(file) |>
    stringr::str_extract(paste0("(?<=\\#\\'\\s{1,2}\\@", tag, "\\s).*")) |>
    na.omit() |>
    as.character() |>
    unique()
}



