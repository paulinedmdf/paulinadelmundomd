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

#' Rename NHANES demographic columns to analysis-friendly names.
#'
#' The `nhanesA` package already returns `RIAGENDR` and `RIDRETH3` as
#' labeled factors ("Male"/"Female", "Non-Hispanic White", ...), so we
#' just alias them and coerce to numeric for age.
recode_demographics <- function(df) {
  df |>
    mutate(
      sex       = RIAGENDR,
      age_years = as.numeric(RIDAGEYR),
      race_eth  = RIDRETH3
    )
}

#' Pool multiple NHANES cycles per CDC analytic guidelines.
#'
#' Pulls each cycle, row-binds them, tags a `cycle` column, and computes
#' the combined-cycle MEC weight as `WTMEC2YR / n_cycles`. Strata (SDMVSTRA)
#' and PSU (SDMVPSU) are passed through unchanged. SEQN does not collide
#' across cycles in NHANES, so vertical concatenation is safe.
#'
#' @param cycle_specs Named list keyed by cycle label (e.g. "2015-2016"),
#'   each value being a named list of `{abbrev = "TABLE_X"}` entries
#'   acceptable to `pull_nhanes_cycle()`.
#' @return A pooled data frame carrying columns `cycle` and `WTMEC4YR`
#'   (or whatever the combined-cycle weight degree should be — for
#'   k cycles the convention is `WTMEC<2k>YR`).
pool_nhanes_cycles <- function(cycle_specs) {
  k <- length(cycle_specs)
  pooled_weight_col <- sprintf("WTMEC%dYR", 2 * k)

  combined <- purrr::imap_dfr(cycle_specs, function(tbls, label) {
    pull_nhanes_cycle(cycle = label, tables = tbls) |>
      dplyr::mutate(cycle = label)
  })
  combined[[pooled_weight_col]] <- combined$WTMEC2YR / k
  combined
}

#' 2013 ACC/AHA Pooled Cohort Equations (Goff et al. 2014, Table A).
#'
#' Inputs are vector-aligned. Returns 10-year ASCVD risk in [0, 1].
#' Out-of-range ages (< 40, > 79) and unsupported races (anything other
#' than non-Hispanic White or non-Hispanic Black) return NA. NHANES race
#' is mapped to "white" or "black" with everyone else NA per the original
#' Goff specification — this is a known equity limitation of PCE itself,
#' not of this implementation.
#'
#' @param age Numeric age in years.
#' @param sex Character "Male" or "Female".
#' @param race Character "White", "Black", or other (becomes NA).
#' @param tc Total cholesterol, mg/dL.
#' @param hdl HDL cholesterol, mg/dL.
#' @param sbp Systolic BP, mm Hg.
#' @param treated Logical: currently on antihypertensive medication.
#' @param smoker Logical: current smoker.
#' @param diabetes Logical: diagnosed diabetes.
pce_risk <- function(age, sex, race, tc, hdl, sbp, treated, smoker, diabetes) {
  n <- length(age)
  out <- rep(NA_real_, n)

  # Coefficient table from Goff 2014 Table A. Each list element follows the
  # "individual sum - mean (Indv-Mean)" form: the linear predictor S = sum
  # of products minus the cohort mean, then 10-yr risk = 1 - baseline^exp(S).
  coef <- list(
    white_male = list(
      ln_age = 12.344, ln_tc = 11.853, ln_age_x_ln_tc = -2.664,
      ln_hdl = -7.990, ln_age_x_ln_hdl = 1.769,
      ln_sbp_t = 1.797, ln_sbp_u = 1.764,
      smoker = 7.837, ln_age_x_smoker = -1.795,
      diabetes = 0.658, mean = 61.18, surv = 0.9144
    ),
    white_female = list(
      ln_age = -29.799, ln_age_sq = 4.884,
      ln_tc = 13.540, ln_age_x_ln_tc = -3.114,
      ln_hdl = -13.578, ln_age_x_ln_hdl = 3.149,
      ln_sbp_t = 2.019, ln_sbp_u = 1.957,
      smoker = 7.574, ln_age_x_smoker = -1.665,
      diabetes = 0.661, mean = -29.18, surv = 0.9665
    ),
    black_male = list(
      ln_age = 2.469, ln_tc = 0.302, ln_hdl = -0.307,
      ln_sbp_t = 1.916, ln_sbp_u = 1.809,
      smoker = 0.549, diabetes = 0.645,
      mean = 19.54, surv = 0.8954
    ),
    black_female = list(
      ln_age = 17.114, ln_tc = 0.940, ln_hdl = -18.920, ln_age_x_ln_hdl = 4.475,
      ln_sbp_t = 29.291, ln_age_x_ln_sbp_t = -6.432,
      ln_sbp_u = 27.820, ln_age_x_ln_sbp_u = -6.087,
      smoker = 0.691, diabetes = 0.874,
      mean = 86.61, surv = 0.9533
    )
  )

  # Map sex/race to a coefficient key; anything unsupported returns NA.
  key <- dplyr::case_when(
    race == "White" & sex == "Male"   ~ "white_male",
    race == "White" & sex == "Female" ~ "white_female",
    race == "Black" & sex == "Male"   ~ "black_male",
    race == "Black" & sex == "Female" ~ "black_female",
    TRUE                              ~ NA_character_
  )

  # Out-of-range filter.
  ok <- !is.na(age) & age >= 40 & age <= 79 &
        !is.na(tc) & !is.na(hdl) & !is.na(sbp) & !is.na(key) &
        !is.na(treated) & !is.na(smoker) & !is.na(diabetes)

  for (i in which(ok)) {
    cf <- coef[[ key[i] ]]
    la <- log(age[i]); lt <- log(tc[i]); lh <- log(hdl[i]); ls <- log(sbp[i])
    smk <- as.integer(smoker[i]); dm <- as.integer(diabetes[i])
    sbp_t <- as.integer(treated[i])

    s <- 0
    if (!is.null(cf$ln_age))             s <- s + cf$ln_age            * la
    if (!is.null(cf$ln_age_sq))          s <- s + cf$ln_age_sq         * la^2
    if (!is.null(cf$ln_tc))              s <- s + cf$ln_tc             * lt
    if (!is.null(cf$ln_age_x_ln_tc))     s <- s + cf$ln_age_x_ln_tc    * la * lt
    if (!is.null(cf$ln_hdl))             s <- s + cf$ln_hdl            * lh
    if (!is.null(cf$ln_age_x_ln_hdl))    s <- s + cf$ln_age_x_ln_hdl   * la * lh
    if (sbp_t == 1) {
      if (!is.null(cf$ln_sbp_t))           s <- s + cf$ln_sbp_t           * ls
      if (!is.null(cf$ln_age_x_ln_sbp_t))  s <- s + cf$ln_age_x_ln_sbp_t  * la * ls
    } else {
      if (!is.null(cf$ln_sbp_u))           s <- s + cf$ln_sbp_u           * ls
      if (!is.null(cf$ln_age_x_ln_sbp_u))  s <- s + cf$ln_age_x_ln_sbp_u  * la * ls
    }
    if (smk == 1) {
      if (!is.null(cf$smoker))           s <- s + cf$smoker
      if (!is.null(cf$ln_age_x_smoker))  s <- s + cf$ln_age_x_smoker * la
    }
    if (dm == 1 && !is.null(cf$diabetes)) s <- s + cf$diabetes

    out[i] <- 1 - cf$surv ^ exp(s - cf$mean)
  }
  out
}
