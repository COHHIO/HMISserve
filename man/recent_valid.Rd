% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/prioritization_utils.R
\name{recent_valid}
\alias{recent_valid}
\title{Retrieve the most recent valid value
Given a timeseries \code{t} and a \code{v}, return the most recent valid value}
\usage{
recent_valid(t, v)
}
\arguments{
\item{t}{\code{(Date/POSIXct)}}

\item{v}{\code{(vector)}}
}
\value{
\code{(vector)} of unique, most recent value(s if there is a tie) of \code{v}
}
\description{
Retrieve the most recent valid value
Given a timeseries \code{t} and a \code{v}, return the most recent valid value
}
