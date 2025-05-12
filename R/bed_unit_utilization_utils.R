
# function for adding bed nights per ee
bed_nights_per_ee <- function(interval, x) {
  is <- lubridate::as_date(lubridate::int_start(interval))
  ie <- lubridate::as_date(lubridate::int_end(interval))
  # if the ee date range and a given interval (in my reporting, a month) overlap,
  dplyr::if_else(lubridate::int_overlaps(x$StayWindow, interval),
                 # then return the difference between
                 as.numeric(difftime(
                   # if the exit date precedes the end of the interval, then the exit
                   # date, otherwise the end of the interval
                   dplyr::if_else(
                     x$ExitAdjust <=  ie,
                     x$ExitAdjust,
                     ie + lubridate::days(1)
                   ),
                   # if the entry date is after the start of the interval, then the
                   # entry date, otherwise the beginning of the interval
                   dplyr::if_else(
                     x$EntryAdjust >= is,
                     x$EntryAdjust,
                     is
                   ),
                   # give it to me in days
                   units = "days"
                 )), 0
  )
}

# function for bed/unit capacity at the record level

unit_capacity <- function(interval, x, multiplier_col = "BedInventory") {
  is <- lubridate::as_date(lubridate::int_start(interval))
  ie <- lubridate::as_date(lubridate::int_end(interval))
  dplyr::if_else(lubridate::int_overlaps(x$AvailableWindow, interval),
                 (as.numeric(difftime(
                   dplyr::if_else(
                     x$InventoryEndAdjust <=  ie,
                     x$InventoryEndAdjust,
                     ie
                   ),
                   dplyr::if_else(
                     x$InventoryStartAdjust >= is,
                     x$InventoryStartAdjust,
                     is
                   ),
                   units = "days"
                 ))+1) * x[[multiplier_col]], 0
  )
}




#' @title Create an interval of the previous n-th month
#'
#' @param n \code{(integer/numeric)}
#'
#' @return \code{(interval)}
#' @export

nth_Month <- function(n) {
  d <- lubridate::`%m-%`(lubridate::floor_date(Sys.Date(), "months"), lubridate:::months.numeric(n))
  out <- lubridate::interval(d, lubridate::ceiling_date(d, "months") - 1)
  rlang::set_names(out, purrr::map(out, lubridate::int_start) |> purrr::map_chr(~paste0(as.character(lubridate::month(.x, label = TRUE)),as.character(lubridate::year(.x)))))
}

#' @title Add Month Counts using a summary function
#'
#' @param x \code{(data.frame)} with necessary functions for `fn` to work.
#' @param n \code{(numeric)} Vector of how many months previous to create summaries for with `fn`
#' @param fn \code{(function)} with which to summarise months. One of `bed_nights_per_ee` or `unit_capacity`
#' @param ... Additional arguments passed to `fn`
#'
#' @return \code{(data.frame)} with columns containing the days the client was enrolled for the month. Columns are named with a three letter month abbreviation followed by a four digit year. IE `Jun2020`
#' @export
#' @seealso bed_nights_per_ee, unit_capacity, nth_Month, bu_sum_months

bu_add_month_counts <- function(x, n = 24, fn = bed_nights_per_ee, ...) {
  nth_Month(1:n) |>
    purrr::map_dfc(fn, x = x, ...) |>
    dplyr::bind_cols(x)
}

#' Title
#'
#' @param x \code{(data.frame)} with month summaries added by `bu_add_month_counts`
#' @param suffix \code{(character)} Suffix to add to the newly created summary columns
#'
#' @return \code{(data.frame)}
#' @export
#'
#' @seealso bed_nights_per_ee, bed_capacity, nth_Month, bu_add_month_counts

bu_sum_months <- function(x, suffix) {
  x |>
    dplyr::group_by(ProjectName, ProjectID, ProjectType) %>%
    dplyr::summarise(
      # BNY = sum(rm_dates$calc$two_yrs_prior_range, na.rm = TRUE),
      dplyr::across(tidyselect::matches("\\w{3}\\d{4}"), sum, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::rename_with(.cols = tidyselect::matches("\\w{3}\\d{4}"), .fn = ~paste0(.x, suffix))
}

#' @title Take the proportion of the Month summaries created by `bu_sum_months`
#'
#' @param x \code{(data.frame)} with the month summaries of usage
#' @param y \code{(data.frame)} with the month summaries of capacity
#'
#' @return \code{(data.frame)} with the proportion of usage/capacity for each month
#' @export

bu_month_proportion <- function(x, y) {
  dividend_suffix <- unique(na.omit(stringr::str_extract(names(x), "\\_\\w+")))
  divisor_suffix <- unique(na.omit(stringr::str_extract(names(y), "\\_\\w+")))
  dplyr::left_join(x,
                   y,
                   by = c("ProjectID", "ProjectName", "ProjectType")) |>
    {\(x) {
      dplyr::bind_cols(
        x |> dplyr::select(-tidyselect::matches("\\w{3}\\d{4}")),
        purrr::map2(x[stringr::str_subset(names(x), paste0("\\w{3}\\d{4}", dividend_suffix))],
                    x[stringr::str_subset(names(x), paste0("\\w{3}\\d{4}", divisor_suffix))], ~
                      .x / .y)
      )
    }}() |>
    dplyr::select(ProjectID, ProjectName, ProjectType, tidyselect::ends_with(dividend_suffix))  |>
    dplyr::rename_with(.cols = tidyselect::ends_with(dividend_suffix), ~stringr::str_remove(.x, dividend_suffix)) |>
    dplyr::mutate(accuracy = .1)
}
