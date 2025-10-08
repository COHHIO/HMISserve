#' Data Quality Check Function Names
#'
#' A character vector containing the names of all data quality check functions
#' available in the HMISserve package.
#'
#' @format A character vector with 60 elements
#' @export
relevant_dq <-
c("dq_check_disability_ssi", "dq_conflicting_hi_ee", "dq_conflicting_income",
"dq_conflicting_unlikely_ncbs", "dq_date_homeless_after_entry",
"dq_detail_missing_disabilities", "dq_dkr_client_veteran_info",
"dq_dkr_destination", "dq_dkr_LoS", "dq_dkr_months_times_homeless",
"dq_dkr_prior_living_situation", "dq_dob", "dq_duplicate_ees",
"dq_future_exits", "dq_hh_active_client_no_hoh",
"dq_hh_children_only", "dq_hh_missing_rel_to_hoh", "dq_hh_no_hoh",
"dq_incorrect_path_contact_date", "dq_invalid_months_times_homeless",
"dq_mahoning_ce_60_days", "dq_missing_approx_date_homeless",
"dq_missing_county_prior", "dq_missing_county_served",
"dq_missing_destination", "dq_missing_hi_entry",
"dq_missing_hi_exit", "dq_missing_income", "dq_missing_months_times_homeless",
"dq_missing_ncbs", "dq_missing_path_contact", "dq_missing_previous_street_ESSH",
"dq_missing_prior_living_situation", "dq_months_homeless_tbd",
"dq_name", "dq_path_enrolled_missing", "dq_path_missing_los_res_prior",
"dq_path_no_status_at_exit", "dq_path_status_determination",
"dq_psh_check_exit_destination", "dq_psh_incorrect_destination",
"dq_psh_missing_project_stay", "dq_race", "dq_referrals_on_hh_members_ssvf",
"dq_referrals_outstanding", "dq_rrh_check_exit_destination",
"dq_rrh_missing_project_stay", "dq_services_rent_paid_no_move_in",
"dq_SOAR_missing_at_exit", "dq_ssn", "dq_ssvf_missing_percent_ami",
"dq_th_check_exit_destination", "dq_th_missing_project_stay",
"dq_th_stayers_bos", "dq_veteran", "dq_veteran_missing_branch",
"dq_veteran_missing_discharge_status", "dq_veteran_missing_year_entered",
"dq_veteran_missing_year_separated", "dq_without_spdats")
