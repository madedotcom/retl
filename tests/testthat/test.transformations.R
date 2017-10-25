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
    res <- expect_warning(safeLookup(dt, dt.lookup, by = "ref"), regexp = "records")

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
