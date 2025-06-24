
n_na <- function(...) {
  x <- data.frame(...)
  apply(x, 1, rlang::as_function(~sum(is.na(.x))))
}

#' @title Find a valid maximum from the input values, or if none is present, return the values
#'
#' @param x \code{(numeric/real)}
#'
#' @return Maximum value
#' @export

valid_max = function(x) {
  out <- na.omit(x)
  if (UU::is_legit(out)) {
    out2 <- max(out)
    if (!is.infinite(out2))
      out <- out2
  } else {
    out <- x
  }
  out
}

#' @title Return a valid Move-In Date
#'
#' @param MoveInDateAdjust \code{(Date)} See `Enrollment_add_Household`
#' @param EntryDate \code{(Date)} Date of Entry for Enrollment
#'
#' @return A data frame with valid Move-In Dates
#' @export

valid_movein_max = function(MoveInDateAdjust, EntryDate) {
  if ((valid_max(MoveInDateAdjust)[1] >= valid_max(EntryDate)[1]) %|% FALSE)
    out <- valid_max(MoveInDateAdjust)
  else
    out <- lubridate::NA_Date_
  out
}

#' @title Retrieve the most recent valid value
#' Given a timeseries `t` and a `v`, return the most recent valid value
#' @param t \code{(Date/POSIXct)}
#' @param v \code{(vector)}
#'
#' @return \code{(vector)} of unique, most recent value(s if there is a tie) of `v`
#' @export

recent_valid <- function(t, v) {
  unique(v[t == valid_max(t[!is.na(v)])])
}
