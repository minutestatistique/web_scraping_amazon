require(rvest)
require(tidyverse)
require(stringr)
require(RSelenium)
require(data.table)
require(openxlsx)
require(doParallel)

proj_path <- file.path("data", "amazon")

log_path <- file.path(proj_path, "out", "log")
if (!dir.exists(log_path)) {
  dir.create(log_path, recursive = TRUE)
}

# parameters setup 
#-------------------------------------------------------------------------------
# page d'accueil
g_base_url <- "https://www.amazon.fr"
# univers Bricolage, hi comme home improvement en anglais
g_url <- paste0(g_base_url, "/gp/bestsellers/hi/ref=zg_bs_nav_0")
# paramètre de la recherche qui affiche la première page de résultat par défaut
g_params <- "?noRedirect=1&page="
# construction de l'URL
g_url <- paste0(g_url, g_params)

# driver Selenium utilisant Google Chrome comme navigateur
g_chrome_version <- "108.0.5359.71"
