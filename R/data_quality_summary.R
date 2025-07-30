#' @title Render Data Quality plots & tables for RminorElevated dq_system_wide
#'
#' @inheritParams data_quality_tables
#' @param dq_past_year \code{(data.frame)} See `data_quality`
#' @param dq_eligibility_detail \code{(data.frame)} See `data_quality`
#' @param dq_overlaps \code{(data.frame)} See `data_quality`
#' @param data_APs \code{(data.frame)} See `data_APs`
#'
#' @return A data frame of data quality summary.
#' @export
#' @include 04_DataQuality.R 04_DataQuality_utils.R 04_DataQuality_summary_utils.R

data_quality_summary <- function(co_clients_served,
                                 rm_dates,
                                 .deps) {
  dq_data <- data_quality(.deps = deps)
  dq_past_year <- dq_data$dq_past_year
  dq_eligibility_detail <- dq_data$dq_eligibility_detail
  dq_overlaps <- dq_data$dq_overlaps

  dq_summary <- list()

  client_summary <- dqu_summary(co_clients_served, distinct = FALSE) |>
    dplyr::rename(`Total Clients` = n)

  dq_summary$projects_errors <- dqu_summary(dq_past_year, filter_exp = Type %in% c("Error", "High Priority") &
                                              !Issue %in% c(
                                                "No Head of Household",
                                                "Missing Relationship to Head of Household",
                                                "Too Many Heads of Household",
                                                "Children Only Household"
                                              ), join = client_summary)

  dq_summary$error_types <- dqu_summary(dq_past_year, filter_exp = Type %in% c("Error", "High Priority"), groups = "Issue", distinct = FALSE)

  dq_summary$projects_warnings <- dqu_summary(dq_past_year, filter_exp = Type == "Warning", distinct = FALSE, join = client_summary)

  dq_summary$warning_types <- dqu_summary(dq_past_year, filter_exp = Type %in% c("Warning"), groups = "Issue", distinct = FALSE)


  dq_summary$hh_issues <- dqu_summary(dq_past_year, filter_exp = Type %in% c("Error", "High Priority") &
                                        Issue %in% c(
                                          "No Head of Household",
                                          "Missing Relationship to Head of Household",
                                          "Too Many Heads of Household",
                                          "Children Only Household"
                                        ), join = client_summary)



  dq_summary$outstanding_referrals <- dqu_summary(dq_past_year, filter_exp = Issue == "Old Outstanding Referral", distinct = FALSE, join = client_summary)


  dq_summary$eligibility <- dqu_summary(HMIS::served_between(dq_eligibility_detail, rm_dates$hc$check_dq_back_to, lubridate::today()), filter_exp = Type == "Warning" & Issue %in% c("Check Eligibility"), join = client_summary)

  dq_summary$clients_without_spdat <- dqu_summary(dq_past_year, filter_exp = Type == "Warning" & Issue %in% c("Non-DV HoHs Entering PH or TH without HARP or SPDAT",
                                                                                                              "HoHs in shelter for 8+ days without HARP or SPDAT"), join = client_summary)

  dq_summary$overlaps <- dqu_summary(HMIS::served_between(dq_overlaps, rm_dates$hc$check_dq_back_to, lubridate::today()), distinct = FALSE, join = client_summary)
  dq_summary$long_stayer <- dqu_summary(dq_past_year, filter_exp = Type == "Warning" & Issue == "Extremely Long Stayer", join = client_summary)

  dq_summary$incorrect_destination <- dqu_summary(dq_past_year, filter_exp = stringr::str_detect(Issue, "Incorrect.*Destination"), join = client_summary)
  dq_summary$psh_destination <- dqu_summary(dq_past_year, filter_exp = stringr::str_detect(Issue, "(?:Destination|Missing).*(?:PSH)"), join = client_summary)

  HMISdata::upload_hmis_data(dq_summary,
                             bucket = "shiny-data-cohhio",
                             folder = "RME",
                             file_name = "dq_summary.rds", format = "rds")

  return(dq_summary)
}


# Plot utils ----
# Mon Sep 20 11:16:46 2021

dqu_plot_theme_labs <- function(g, x = NULL, y = NULL) {
  .labs <- list()
  if (UU::is_legit(x))
    .labs <- list(x = x)
  if (UU::is_legit(y))
    .labs <- list(x = y)
  g +
    ggplot2::geom_col(show.legend = FALSE) +
    ggplot2::coord_flip() +
    do.call(ggplot2::labs, .labs) +
    ggplot2::scale_fill_viridis_c(direction = -1) +
    ggplot2::theme_minimal(base_size = 18)
}

dqu_summary <- function(.data, filter_exp, groups = c("ProjectName", "ProjectID"), distinct = TRUE, x_label = "", y_label = "", data = TRUE, join, suffix = c("_Issue", "_Clients")) {
  p_data <- .data
  if (!missing(filter_exp))
    p_data <- p_data %>%
      dplyr::filter(
        !!rlang::enquo(filter_exp)
      )
  if (distinct)
    p_data <- dplyr::distinct(p_data, PersonalID, ProjectID, ProjectName)
  if (!missing(join))
    ns <- purrr::map(suffix, ~rlang::sym(paste0("n", .x)))
  else
    ns <- list(rlang::sym("n"))
  p_data <- dplyr::group_by(p_data, !!!purrr::map(groups, rlang::sym)) %>%
    dplyr::summarise(!!ns[[1]] := dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(!!ns[[1]]))

  if (!missing(join)) {
    p_data <- dplyr::left_join(p_data, join, by = UU::common_names(p_data, join))
    ns <- purrr::map(stringr::str_subset(names(p_data), stringr::regex("(?:^n_)|(?:Clients$)", ignore_case = TRUE)), rlang::sym)
    ex <- rlang::expr(round(!!ns[[1]] / !!ns[[2]], 5))

    p_data <- dplyr::mutate(p_data, `Frequency (Errors / Total Clients)` = !!ex,
                            from_mean = dplyr::percent_rank(`Frequency (Errors / Total Clients)`) - .5)
  }

  if (!data) {
    if (all(c("ProjectName", "ProjectID") %in% groups))
      p_data <- dplyr::mutate(p_data, hover = paste0(ProjectName, ":", ProjectID))

    x_order <- purrr::when("hover" %in% names(p_data), . ~ rlang::expr(hover), ~ rlang::expr(Issue))
    out <-
      ggplot2::ggplot(
        utils::head(p_data, 20L),
        ggplot2::aes(
          x = stats::reorder(!!x_order, n),
          y = n,
          fill = n
        )
      ) |>
      dqu_plot_theme_labs(x = x_label, y = y_label)
  } else
    out <- p_data

  out
}
