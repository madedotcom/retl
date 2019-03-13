context("Code Style")

if (requireNamespace("lintr", quietly = TRUE)) {
  test_that("Package Style", {
    lintr::expect_lint_free(
      linters = c(
        lintr::commas_linter,
        lintr::line_length_linter,
        lintr::no_tab_linter,
        lintr::assignment_linter,
        lintr::closed_curly_linter,
        lintr::absolute_path_linter,
        lintr::object_name_linter(c("lowerCamelCase", "dotted.case"))
      )
    )
  })
}
