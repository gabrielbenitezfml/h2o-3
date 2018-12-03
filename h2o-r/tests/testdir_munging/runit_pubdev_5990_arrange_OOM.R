setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")
##
# PUBDEV-5990
# arrange function exploded in memory when performing h2o.arrange.  Thanks to Lauren Diperna for the repo.
##

test.merge <- function() {
  nrow = 20*8023940
  browser()
  df = h2o.createFrame(rows = nrow, cols = 320, binary_fraction = 0, categorical_fraction = 0,
                       string_fraction = 0, time_fraction = 0, integer_fraction = 1, integer_range = 3, missing_fraction = .9)
  
  df_col = h2o.createFrame(rows = nrow, cols = 1, binary_fraction = 0, categorical_fraction = 0,
                           string_fraction = 0, time_fraction = 0, integer_fraction = 0, real_range = 2, missing_fraction = 0)
  
  df_col = h2o.ifelse(df_col <=0, .5, df_col)
  colnames(df_col) = c("sort_col")
  
  # cbind the two
  df = h2o.cbind(df, df_col)
  h2o.remove(df_col)
  sorted <- h2o.arrange(df, desc(sort_col))
  h2o.head(sorted)  # this will hang according to Lauren Diperna
}

doTest("Test out the merge() functionality", test.merge)
