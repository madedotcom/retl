context("Google sheet functions")

test_that("Check gsLoadSheet load sheet as expected", {
  x <- gsLoadSheet(
    key = "1OMwQKIkhlwmcih--ANYdzx_g8XgkXNOwsV_jfE6k0Xg",
    tab = "Class Data"
  )
  expect_equal(
    nrow(x),
    30,
    label = "30 rows in tab Class Data in sheet 1OMwQKIkhlwmcih--ANYdzx_g8XgkXNOwsV_jfE6k0Xg"
  )
  expect_equal(
    ncol(x),
    6,
    label = "30 rows in tab Class Data in sheet 1OMwQKIkhlwmcih--ANYdzx_g8XgkXNOwsV_jfE6k0Xg"
  )
})

test_that("Check gsLoadAll load sheet as expected", {
  x <- gsLoadAll(
    key = "1OMwQKIkhlwmcih--ANYdzx_g8XgkXNOwsV_jfE6k0Xg"
  )
  expect_equal(
    length(x),
    2,
    label = "2 tabs in publish sheet"
  )
  y <- rbindlist(x)
  expect_equal(
    nrow(y),
    60,
    label = "30 rows in public sheet"
  )
  expect_equal(
    ncol(y),
    6,
    label = "6 columns in public sheet"
  )
})
