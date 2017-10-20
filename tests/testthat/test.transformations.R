context("Transformations")

test_that("Safe lookup works", {
    dt <- data.table(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt.lookup <- data.table(ref = c(1L, 2L, 3L), value = c("a", "b", NA_character_))
    res <- safeLookup(dt, dt.lookup, by = "ref")
    expect <- c("b", NA_character_, "b")
    expect_equal(res$value, expect)


    dt <- data.table(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt.lookup <- data.table(ref = c(1L, 2L, 3L, 2L), value = c("a", "b", NA_character_, "c"))
    res <- expect_error(safeLookup(dt, dt.lookup, by = "ref"), regexp = "unique")

    dt <- data.table(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt <- dt[id == 0]
    dt.lookup <- data.table(ref = c(0), value = c(0))
    res <- expect_error(safeLookup(dt, dt.lookup, by = "ref"), regexp = "recrods")

    dt <- data.frame(id = c(1L, 2L, 3L), ref = c(2L, 3L, 2L))
    dt.lookup <- data.frame(ref = c(1L, 2L, 3L), value = c("a", "b", NA_character_))
    res <- expect_error(safeLookup(dt, dt.lookup, by = "ref"), 'data.frame')

})
