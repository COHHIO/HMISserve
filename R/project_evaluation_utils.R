
merge_projects <- function(x, to_merge) {
  var <- rlang::enexpr(x)
  for (i in seq_along(to_merge)) {
    idx <- which(x %in% to_merge[[i]][[var]])
    if (UU::is_legit(idx))
      x[idx] <- switch(rlang::expr_deparse(var),
                       ProjectName = names(to_merge)[i],
                       # the first projectID multipled by 1000 (current ProjectIDs are just over 1100, so this should be fine given the lowest projectID is 2)
                       ProjectID = as.character(as.numeric(to_merge[[i]][[var]][1]) * 1000))
  }
  x
}

peval_filter_select <- function(x,
                                vars,
                                Enrollment_extra_Client_Exit_HH_CL_AaE,
                                ...,
                                stayed = FALSE,
                                served = FALSE,
                                exited = FALSE,
                                entered = FALSE,
                                arrange = TRUE,
                                distinct = TRUE,
                                start = rm_dates$hc$project_eval_start,
                                end = rm_dates$hc$project_eval_end
) {

  addtl_filters <- rlang::enexprs(...)

  out <- x

  if (served)
    out <- out |> HMIS::served_between(start, end)
  if (stayed)
    out <- out |> HMIS::stayed_between(start, end)
  if (exited)
    out <- out |> HMIS::exited_between(start, end)
  if (entered)
    out <- out |> HMIS::entered_between(start, end)

  out <- out |>
    dplyr::select("PersonalID", "ProjectID", "EnrollmentID") %>%
    dplyr::inner_join(pe_coc_funded, by = "ProjectID") %>%
    dplyr::left_join(
      Enrollment_extra_Client_Exit_HH_CL_AaE %>%
        dplyr::mutate(ProjectType = as.character(ProjectType)) |>
        dplyr::select(- tidyselect::all_of(c("UserID",
                                             "DateCreated",
                                             "DateUpdated",
                                             "DateDeleted",
                                             "ExportID"))),
      by = c(
        "PersonalID",
        "EnrollmentID",
        "ProjectID",
        "ProjectType",
        "ProjectName"
      )
    )

  if (UU::is_legit(addtl_filters))
    out <- dplyr::filter(out, !!!addtl_filters)

  out <- dplyr::select(out, dplyr::all_of(vars))

  if (arrange)
    out <- dplyr::arrange(out, PersonalID, AltProjectID, dplyr::desc(EntryDate))
  if (distinct)
    out <- dplyr::distinct(out, PersonalID, AltProjectName, .keep_all = TRUE)
  # no dupes w/in a project
  out
}

peval_summary <- function(x, nm, pe_coc_funded) {
  if (missing(nm))
    nm <- rlang::expr_deparse(rlang::enexpr(x))
  
  nm <- nm |>
    stringr::str_extract("(?<=summary\\_)[\\w\\_]+") |>
    rlang::sym()
  
  x |>
    dplyr::group_by(AltProjectID) |>
    dplyr::summarise(!!nm := dplyr::n(), .groups = "drop") |>
    dplyr::right_join(unique(pe_coc_funded["AltProjectID"]), by = "AltProjectID") |>
    tidyr::replace_na(rlang::list2(!!nm := 0L))
}

peval_math <- function(var, project_eval_due) {

  dplyr::case_when(
    lubridate::today() <= project_eval_due &
      is.na({{var}}) ~
      paste0(
        "Documents either not yet received or not yet processed. They are due ", format(project_eval_due, "%A %b %e, %Y"),"."
      ),
    lubridate::today() > project_eval_due &
      is.na({{var}}) ~
      paste0(
        "Documentation either not yet received or not yet processed by the CoC Team. They were due ", format(project_eval_due, "%A %b %e, %Y"), "."
      ),
    {{var}} > project_eval_due ~
      "Documentation received past deadline.",
    {{var}} <= project_eval_due ~
      "Your documentation was reviewed by the CoC team and scored. Please contact <a href='mailto:ohioboscoc@cohhio.org' target='_blank'>ohioboscoc@cohhio.org</a> if you have questions about your scoring."
  )

}
