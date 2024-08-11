source(file.path("src", "R", "utl.R"), encoding = "UTF-8")
source(file.path("src", "R", "amazon_global.R"), encoding = "UTF-8")
source(file.path("src", "R", "amazon_utl.R"), encoding = "UTF-8")

on.exit(
  {
    map(
      rsd_lst,
      ~ system(
        paste0("Taskkill /F /T" ," /PID ", .x$server$process$get_pid())
      )
    )
  }
)

log_file <- file(
  file.path(
    log_path,
    paste0(
      "amazon_main_", format(Sys.time(), "%Y-%m-%d_%H%M%S.log")
    )
  ),
  open = "wt"
)
sink(log_file, type = "output")
sink(log_file, type = "message")
print(paste(t <- Sys.time(), "START..."))

# récupérer les différents niveaux de catégories
#-------------------------------------------------------------------------------
# sync
t_1 <- print_function_header(
  "amazon_get_sub_urls_sync()"
)
res <- amazon_get_rd(
  n_cores = 1,
  chrome_version = g_chrome_version,
  url_raw = g_base_url,
  sleep = 10
)
# re-create driver(s) each time a function uses it(them), otherwise use advanced
# parameters in remoteDriver() Java function
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_sub_urls_sync(
  rd = rd_lst[[1]],
  url_raw = g_base_url,
  url_init = g_url,
  out_path = file.path(proj_path, "out", "urls_levels", "sync")
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# async
t_1 <- print_function_header(
  "amazon_get_sub_urls_async()"
)
res <- amazon_get_rd(
  n_cores = 4,
  chrome_version = g_chrome_version,
  url_raw = g_base_url
)
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_sub_urls_async(
  rd_lst = rd_lst,
  url_raw = g_base_url,
  url_init = g_url,
  out_path = file.path(proj_path, "out", "urls_levels")
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# process urls_levels
#-------------------------------------------------------------------------------
t_1 <- print_function_header(
  "amazon_process_sub_urls()"
)
amazon_process_sub_urls(
  in_path = file.path(proj_path, "out", "urls_levels")
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# get urls_levels labels
#-------------------------------------------------------------------------------
t_1 <- print_function_header(
  "amazon_get_urls_labels()"
)
res <- amazon_get_rd(
  n_cores = 4,
  chrome_version = g_chrome_version,
  url_raw = g_base_url
)
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_urls_labels(
  in_path = file.path(proj_path, "out", "urls_levels", "all"),
  url_raw = g_base_url,
  rd_lst = rd_lst
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# get prd_urls
#-------------------------------------------------------------------------------
t_1 <- print_function_header(
  'amazon_get_prd_urls(version = "async")'
)
res <- amazon_get_rd(
  n_cores = 4,
  chrome_version = g_chrome_version,
  url_raw = g_base_url
)
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_prd_urls(
  in_path = file.path(proj_path, "out", "urls_levels", "all"),
  out_path = file.path(proj_path, "out", "prd_urls", "async"),
  url_raw = g_base_url,
  rd_lst = rd_lst,
  version = "async"
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

t_1 <- print_function_header(
  'amazon_get_prd_urls(version = "sync")'
)
res <- amazon_get_rd(
  n_cores = 1,
  chrome_version = g_chrome_version,
  url_raw = g_base_url,
  sleep = 10
)
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_prd_urls(
  in_path = file.path(proj_path, "out", "urls_levels", "all"),
  out_path = file.path(proj_path, "out", "prd_urls", "sync"),
  url_raw = g_base_url,
  rd_lst = rd_lst,
  version = "sync"
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# process prd_urls
#-------------------------------------------------------------------------------
t_1 <- print_function_header(
  "amazon_process_prd_urls()"
)
amazon_process_prd_urls(
  prd_path = file.path(proj_path, "out", "prd_urls"),
  url_path = file.path(proj_path, "out", "urls_levels", "all")
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# get prd_chars
#-------------------------------------------------------------------------------
t_1 <- print_function_header(
  'amazon_get_prd_chars(version = "async")'
)
res <- amazon_get_rd(
  n_cores = 4,
  chrome_version = g_chrome_version,
  url_raw = g_base_url
)
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_prd_chars(
  in_path = file.path(proj_path, "out", "prd_urls", "all"),
  out_path = file.path(proj_path, "out", "prd_chars", "async"),
  url_raw = g_base_url,
  rd_lst = rd_lst,
  version = "async"
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

t_1 <- print_function_header(
  'amazon_get_prd_chars(version = "sync")'
)
res <- amazon_get_rd(
  n_cores = 1,
  chrome_version = g_chrome_version,
  url_raw = g_base_url,
  sleep = 10
)
rsd_lst <- res$rsd_lst
rd_lst <- res$rd_lst
amazon_get_prd_chars(
  in_path = file.path(proj_path, "out", "prd_urls", "all"),
  out_path = file.path(proj_path, "out", "prd_chars", "sync"),
  url_raw = g_base_url,
  rd_lst = rd_lst,
  version = "sync"
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

# process prd_chars
#-------------------------------------------------------------------------------
t_1 <- print_function_header(
  "amazon_process_sub_urls()"
)
amazon_process_prd_chars(
  prd_path = file.path(proj_path, "out", "prd_chars"),
  url_path = file.path(proj_path, "out", "urls_levels", "all")
)
print(difftime(Sys.time(), t_1))
cat("\n")
gc()

print("END")
print(difftime(Sys.time(), t))
cat("\n")
sink()