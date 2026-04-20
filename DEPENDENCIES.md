# Environment

Reproducibility for the Quarto analyses under `projects/`.

## R (>= 4.3)

```r
install.packages(c(
  "nhanesA",      # CDC NHANES data pull
  "survey",       # complex survey design (required for NHANES)
  "srvyr",        # tidyverse-friendly survey
  "dplyr", "tidyr", "ggplot2", "forcats", "stringr", "readr",
  "gt", "gtsummary",
  "knitr", "rmarkdown", "reticulate"
))
```

## Python (>= 3.11)

```
pip install -r requirements.txt
```

## Quarto (>= 1.4)

Install from <https://quarto.org/docs/get-started/>. Render any project with:

```
quarto render projects/01-cardiometabolic-risk/analysis.qmd
```
