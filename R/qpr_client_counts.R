#' Title
#'
#' @param Project
#' @param Enrollment_extra_Client_Exit_HH_CL_AaE
#' @param Disabilities
#' @param Referrals
#' @param rm_dates
#'
#' @return
#' @export
#'
#' @examples
client_counts <- function(Project,
                          Enrollment_extra_Client_Exit_HH_CL_AaE,
                          Disabilities,
                          Referrals,
                          rm_dates) {

  project_small <- qpr_project_small(Project, rm_dates)

  enrollment_small <- qpr_enrollment_small(Enrollment_extra_Client_Exit_HH_CL_AaE)

  validation <- qpr_validation(project_small, enrollment_small)

  mental_health_unsheltered <-  qpr_mental_health(validation, Disabilities)

  path_referrals <- qpr_path_to_rrhpsh(Enrollment_extra_Client_Exit_HH_CL_AaE, Referrals)

  HMISdata::upload_hmis_data(project_small,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "project_small.parquet", format = "parquet")
  HMISdata::upload_hmis_data(enrollment_small,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "enrollment_small.parquet", format = "parquet")
  HMISdata::upload_hmis_data(path_referrals,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "path_referrals.parquet", format = "parquet")
  HMISdata::upload_hmis_data(mental_health_unsheltered,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "mental_health_unsheltered.parquet", format = "parquet")
  HMISdata::upload_hmis_data(validation,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "validation.parquet", format = "parquet")
}
