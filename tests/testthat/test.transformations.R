context("Transformations")

test_that("Safe lookup works", {
    dt <- data.table(id = c(1L, 2L, 3L, 4L), ref = c(2L, 3L, 2L, 4L))
    dt.lookup <- data.table(ref = c(4L, 1L, 2L, 3L), value = c("c", "a", "b", NA_character_))
    res <- safeLookup(dt, dt.lookup, by = "ref")
    expect <- c("b", NA_character_, "b", "c")
    expect_equal(res$value, expect)

    dt <- data.table(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt.lookup <- data.table(ref = c(1L, 2L, 3L, 2L), value = c("a", "b", NA_character_, "c"))
    res <- expect_error(safeLookup(dt, dt.lookup, by = "ref"), regexp = "unique")

    dt <- data.table(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt <- dt[id == 0]
    dt.lookup <- data.table(ref = c(0), value = c(0))
    expect_warning(res <- safeLookup(dt, dt.lookup, by = "ref"), regexp = "records")
    expect_equal(colnames(res), c("id", "ref", "value"))

    dt <- data.frame(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt.lookup <- data.frame(ref = c(1L, 2L, 3L), value = c("a", "b", NA_character_))
    res <- expect_error(safeLookup(dt, dt.lookup, by = "ref"), 'data.frame')

    # can join by two columns
    dt <- data.table(id = c(1L, 2L, 3L, 4L, 5L), ref = c(2L, 3L, 2L, 4L, 3L), ref.b = c(12L, 13L, 12L, 14L, 14L))
    dt.lookup <- data.table(ref = c(4L, 1L, 2L, 3L, 3L), ref.b = c(14L, 11L, 12L, 13L, 14L), value = c("c", "a", "b", NA_character_, "x"))
    res <- safeLookup(dt, dt.lookup, by = c("ref", "ref.b"))
    expect <- c("b", NA_character_, "b", "c", "x")
    expect_equal(res$value, expect)

    # Merging to create a new object doesn't alter the original tables.
    dt <- data.table(id = c(1L, 2L, 3L, 4L, 5L), ref = c(2L, 3L, 2L, 4L, 3L), ref.b = c(12L, 13L, 12L, 14L, 14L))
    dt.lookup <- data.table(ref = c(4L, 1L, 2L, 3L, 3L), ref.b = c(14L, 11L, 12L, 13L, 14L), value = c("c", "a", "b", NA_character_, "x"))
    res <- safeLookup(dt, dt.lookup, by = c("ref", "ref.b"))
    expect_true(!"000000" %in% colnames(dt))
})

test_that("Header transformation works", {
  dt <- data.table("_under_SCORE_" = "underscore",
                   "-da-Sh-" = "dash",
                   " sP ace " = "space",
                   "C-om bin.ed" = "combined")
  res <- conformHeader(names(dt))
  expect <- c(".under.score.", ".da.sh.", ".sp.ace.", "c.om.bin.ed")
  expect_identical(res, expect)

  res <-  conformHeader(names(dt), separator = "_")
  expect <- c("_under_score_", "_da_sh_", "_sp_ace_", "c_om_bin_ed")
  expect_identical(res, expect)
})

test_that("disaggregate", {
  # Correct number of rows.
  df <- read.table(
    text = "
    sku weight
    CUSISLA03BLU-UK 3
    RUGISIS04BEI-UK 4
    TBLBRM001WHI-UK 0",
    header = T,
    stringsAsFactors = F
  )

  calculated <- disaggregate(table = df, by = "weight")
  expected <- read.table(
    text = "
    sku weight
    CUSISLA03BLU-UK 1
    CUSISLA03BLU-UK 1
    CUSISLA03BLU-UK 1
    RUGISIS04BEI-UK 1
    RUGISIS04BEI-UK 1
    RUGISIS04BEI-UK 1
    RUGISIS04BEI-UK 1",
    header = T,
    stringsAsFactors = F
  )
  expect_equal(calculated, expected,
               label = "Each line is split into multiple matching the weight.")

  # Data table retains its class.
  df <- read.table(
    text = "
    sku weight
    CUSISLA03BLU-UK 3
    RUGISIS04BEI-UK 4
    TBLBRM001WHI-UK 0",
    header = T,
    stringsAsFactors = F
  )
  dt <- data.table(df)
  calculated <- disaggregate(table = dt, by = "weight")
  expect_equal(sum(calculated$weight), sum(dt$weight),
               label = "The total weight matches that of the original.")
  expect_true(is.data.table(calculated),
              label = "The data table class is retained.")
})
