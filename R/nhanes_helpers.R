suppressPackageStartupMessages({
  library(nhanesA)
  library(dplyr)
  library(survey)
  library(srvyr)
})

#' Pull and merge NHANES files for one cycle.
#'
#' @param cycle Two-letter cycle suffix (e.g., "J" for 2017-2018, "I" for 2015-2016).
#' @param tables Named list: list(DEMO = "DEMO_J", BMX = "BMX_J", ...).
#' @return A single joined data frame keyed on SEQN.
pull_nhanes_cycle <- function(cycle, tables) {
  dfs <- lapply(tables, function(tbl) {
    nhanes(tbl)
  })
  Reduce(function(x, y) dplyr::full_join(x, y, by = "SEQN"), dfs)
}

#' Build a complex-survey design object for NHANES.
#'
#' @param data Data frame with NHANES sampling weights and design variables.
#' @param weight Column name of the sampling weight (e.g., "WTMEC2YR").
#' @return A `srvyr` survey design ready for `summarize`/`svyglm`.
nhanes_design <- function(data, weight = "WTMEC2YR") {
  data |>
    srvyr::as_survey_design(
      ids     = SDMVPSU,
      strata  = SDMVSTRA,
      weights = !!rlang::sym(weight),
      nest    = TRUE
    )
}

#' Recode NHANES demographic columns into analysis-ready factors.
recode_demographics <- function(df) {
  df |>
    mutate(
      sex       = factor(RIAGENDR, levels = c(1, 2), labels = c("Male", "Female")),
      age_years = RIDAGEYR,
      race_eth  = factor(
        RIDRETH3,
        levels = c(1, 2, 3, 4, 6, 7),
        labels = c(
          "Mexican American", "Other Hispanic", "NH White", "NH Black",
          "NH Asian", "Other / Multi"
        )
      )
    )
}
