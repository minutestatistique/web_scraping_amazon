string_cleaning <- function (v) {
  sapply(
    v,
    function (e) {
      trimws(
        gsub(
          "\n",
          "",
          gsub(
            "\U0001F1EA",
            "",
            gsub(
              "\U0001F1FA",
              "",
              e
            )
          )
        ),
        which = "both"
      )
    }
  )
}

print_function_header <- function (message) {
  print(paste(rep("-", 80), collapse = ""))
  print(message)
  print(paste(rep("-", 80), collapse = ""))
  Sys.time()
}
