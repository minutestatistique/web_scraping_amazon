amazon_get_rd <- function (
  n_cores,
  chrome_version,
  url_raw,
  sleep = 40
) {
  rsd_lst <- map(
    seq_len(n_cores),
    ~ rsDriver(
      browser = "chrome",
      port = as.integer(4541 + .x),
      chromever = chrome_version
    )
  )
  rd_lst <- map(
    rsd_lst,
    ~ .x[["client"]]
  )
  map(
    seq_len(n_cores),
    ~ rd_lst[[.x]]$navigate(url_raw)
  )
  Sys.sleep(sleep)
  list(
    rsd_lst = rsd_lst,
    rd_lst = rd_lst
  )
}

# récupérer les liens URL d'un lien URL
amazon_html_get_sub_urls <- function (
  rd,
  url_in,
  pattern
) {
  rd$navigate(url_in)
  page <- read_html(unlist(rd$getPageSource()))
  urls_out <- data.table(
    URL = page %>%
      html_nodes("*") %>%
      html_attr("href")
  )
  urls_out[grepl(pattern, URL, perl = TRUE), ]
}

# récupérer les sous-catégories d'une catégorie
amazon_get_sub_urls_sync <- function (
  rd,
  url_raw,
  url_init,
  out_path,
  levels = 1:9
) {
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }
  for (l in levels) {
    print(paste("level", l))
    if (l == 1) {
      urls_in <- amazon_html_get_sub_urls(
        rd = rd,
        url_in = url_init,
        pattern = paste0("nav_hi_", l)
      )[, URL]
    } else {
      if (paste0("urls_", l - 1, "_", l) %in% ls()) {
        urls_in <- get(paste0("urls_", l - 1, "_", l))[
          ,
          unique(get(paste0("URL_", l)))
        ]
      } else {
        urls_in <- character(0)
      }
    }
    if (length(urls_in) > 0) {
      assign(
        paste0("urls_", l, "_", l + 1),
        rbindlist(
          lapply(
            urls_in,
            function (u) {
              urls_out <- amazon_html_get_sub_urls(
                rd = rd,
                url_in = paste0(url_raw, u),
                pattern = paste0("nav_hi_", l + 1)
              )
              urls_out[, URL_IN := u]
              setnames(
                urls_out,
                c(
                  "URL_IN",
                  "URL"
                ),
                paste0(
                  "URL_",
                  c(
                    l,
                    l + 1
                  )
                )
              )
            }
          )
        )
      )
      fwrite(
        get(paste0("urls_", l, "_", l + 1)),
        file = file.path(
          out_path,
          paste0("urls_", l, "_", l + 1, ".csv.gz")
        )
      )
    }
  }
}

amazon_get_sub_urls_async <- function (
  rd_lst,
  url_raw,
  url_init,
  out_path,
  levels = 1:9
) {
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }
  registerDoParallel(cl <- makeCluster(length(rd_lst)))
  on.exit(
    stopCluster(cl)
  )
  foreach(
    i = seq_len(length(rd_lst)),
    .packages = c(
      "rvest",
      "RSelenium",
      "data.table",
      "stringr"
    ),
    .export = c(
      "amazon_html_get_sub_urls",
      "amazon_get_sub_urls_sync"
    )
  ) %dopar% {
    amazon_get_sub_urls_sync(
      rd = rd_lst[[i]],
      url_raw = url_raw,
      url_init = url_init,
      out_path = file.path(out_path, paste0("async_", i)),
      levels = levels
    )
  }
}

amazon_process_sub_urls <- function (
  in_path
) {
  my_tries <- setdiff(
    list.files(in_path),
    "all"
  )
  out_path <- file.path(in_path, "all")
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }
  my_files <- list.files(
    file.path(
      in_path,
      my_tries[[1]]
    )
  )
  for (f in my_files) {
    assign(
      gsub(".csv.gz", "", f, fixed = TRUE),
      unique(
        rbindlist(
          lapply(
            my_tries,
            function (t) {
              fread(
                file.path(
                  in_path,
                  t,
                  f
                )
              )
            }
          )
        )
      )
    )
    fwrite(
      get(gsub(".csv.gz", "", f, fixed = TRUE)),
      file = file.path(out_path, f)
    )
  }
  my_dt <- gsub(
    ".csv.gz",
    "",
    my_files,
    fixed = TRUE
  )
  for (dt_name in my_dt) {
    print(dt_name)
    if (dt_name == "urls_1_2") {
      urls <- get(dt_name)
    } else {
      if (nrow(get(dt_name)) > 0) {
        sup <- last(
          unlist(
            strsplit(
              dt_name,
              "_",
              fixed = TRUE
            )
          )
        )
        urls <- merge(
          urls,
          get(dt_name),
          by = paste0(
            "URL_",
            unlist(
              strsplit(
                dt_name,
                "_",
                fixed = TRUE
              )
            )[[2]]
          ),
          all = TRUE
        )
      }
    }
  }
  write.xlsx(
    setcolorder(urls, paste0("URL_", 1:sup)),
    file = file.path(out_path, "urls_levels.xlsx"),
    overwrite = TRUE
  )
}

amazon_html_get_urls_levels_labels <- function (rd, url_raw, urls) {
  for (r in seq_len(nrow(urls))) {
    if (urls[r, !is.na(URL)]) {
      rd$navigate(paste0(url_raw, urls[r, URL]))
      page <- read_html(unlist(rd$getPageSource()))
      res <- page %>%
        html_elements(".a-text-bold") %>%
        html_text()
      urls[
        r,
        URL_LABEL := res
      ]
    }
  }
  urls
}

amazon_get_urls_labels <- function (
  in_path,
  url_raw,
  rd_lst
) {
  urls_levels <- as.data.table(
    read.xlsx(
      file.path(in_path, "urls_levels.xlsx")
    )
  )
  urls <- unique(
    rbindlist(
      lapply(
        names(urls_levels),
        function (e) {
          data.table(
            URL = urls_levels[, unique(get(e))]
          )
        }
      )
    )
  )[!is.na(URL), ]
  width <- ceiling(urls[, .N / length(rd_lst)])
  registerDoParallel(cl <- makeCluster(length(rd_lst)))
  res_all <- rbindlist(
    foreach(
      i = seq_len(length(rd_lst)),
      .packages = c("rvest", "RSelenium", "data.table", "stringr"),
      .export = c(
        "amazon_html_get_urls_levels_labels"
      )
    ) %dopar% {
      res <- amazon_html_get_urls_levels_labels(
        rd = rd_lst[[i]],
        url_raw = url_raw,
        urls = urls[((i - 1) * width + 1):min(i * width, .N), ]
      )
      res[
        ,
        URL_LABEL := gsub(
          "Les meilleures ventes en ",
          "",
          URL_LABEL,
          fixed = TRUE
        )
      ]
      fwrite(
        res,
        file = file.path(
          in_path,
          paste0("urls_labels_", i, ".csv.gz")
        )
      )
      res
    },
    use.names = TRUE,
    fill = TRUE
  )
  write.xlsx(
    res_all,
    file = file.path(in_path, "urls_labels_async.xlsx"),
    overwrite = TRUE
  )
}

amazon_html_get_prd_pages <- function (rd, url_in) {
  rd$navigate(url_in)
  page <- read_html(unlist(rd$getPageSource()))
  urls_out <- data.table(
    URL = page %>%
      html_nodes(".a-text-bold , .a-selected a , .a-last a") %>%
      # html_nodes("._cDEzb_p13n-sc-css-line-clamp-4_2q2cc") %>%
      html_attr("href")
  )
  urls_out[!is.na(URL), ]
}

amazon_html_get_prd_lst <- function (rd, url_in, sleep) {
  rd$navigate(url_in)
  rd$executeScript(
    script = "window.scrollTo(0, document.body.scrollHeight)"
  )
  # Sys.sleep(sleep)
  rd$executeScript(
    script = "window.scrollTo(document.body.scrollHeight, 0)"
  )
  # Sys.sleep(sleep)
  rd$executeScript(
    script = "window.scrollTo(0, document.body.scrollHeight)"
  )
  Sys.sleep(sleep)
  page <- read_html(unlist(rd$getPageSource()))
  urls_prd <- data.table(
    URL = page %>%
      html_nodes("a.a-link-normal") %>%
      html_attr("href")
  )
  urls_prd <- urls_prd[!is.na(URL), ]
  unique(
    urls_prd[
      # grepl("pd_rd_i", URL, perl = TRUE) &
      grepl("psc=1", URL, perl = TRUE) &
        !grepl("product-reviews", URL, perl = TRUE),
    ]
  )
}

amazon_html_get_prd_urls <- function (rd, urls_in, base_url, sleep) {
  rbindlist(
    lapply(
      urls_in,
      function (u) {
        urls_pages <- amazon_html_get_prd_pages(
          rd,
          paste0(base_url, u)
        )
        if (nrow(urls_pages) > 0) {
          setnames(
            rbindlist(
              list(
                amazon_html_get_prd_lst(
                  rd,
                  paste0(base_url, urls_pages[1, URL]),
                  sleep
                ),
                amazon_html_get_prd_lst(
                  rd,
                  paste0(base_url, urls_pages[2, URL]),
                  sleep
                )
              )
            ),
            "URL",
            "PRD_URL"
          )[, URL := u]
        } else {
          data.table(PRD_URL = character(0), URL = character(0))
        }
      }
    )
  )
}

amazon_get_prd_urls <- function(
  in_path,
  out_path,
  url_raw,
  rd_lst,
  version = "async"
) {
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }
  urls <- as.data.table(
    read.xlsx(
      file.path(in_path, "urls_levels.xlsx")
    )
  )
  levels <- as.numeric(
    gsub(
      "URL_",
      "",
      names(urls),
      fixed = TRUE
    )
  )
  if (max(levels) > 1) {
    for (l in 1:max(levels)) {
      if (l == max(levels)) {
        urls[
          !is.na(get(paste0("URL_", l))),
          DEPTH := l
        ]
      } else {
        urls[
          !is.na(get(paste0("URL_", l))) & is.na(get(paste0("URL_", l + 1))),
          DEPTH := l
        ]
      }
    }
  } else {
    urls[, DEPTH := 1]
  }
  for (l in levels) {
    urls[
      DEPTH == l,
      URL := get(
        paste0(
          "URL_",
          l
        )
      )
    ]
  }
  if (version == "sync") {
    if (max(levels) > 1) {
      for (l in levels) {
        print(paste("level", l))
        prd_urls <- amazon_html_get_prd_urls(
          rd = rd_lst[[1]],
          urls_in = urls[DEPTH == l, URL],
          base_url = url_raw,
          sleep = 3
        )
        fwrite(
          prd_urls,
          file = file.path(out_path, paste0("prd_urls_sync_depth_", l, ".csv.gz"))
        )
      }
    }
  } else {
    width <- ceiling(urls[, .N / length(rd_lst)])
    registerDoParallel(cl <- makeCluster(length(rd_lst)))
    foreach(
      i = seq_len(length(rd_lst)),
      .packages = c("rvest", "RSelenium", "data.table", "stringr"),
      .export = c(
        "amazon_html_get_prd_urls",
        "amazon_html_get_prd_pages",
        "amazon_html_get_prd_lst"
      )
    ) %dopar% {
      prd_urls <- amazon_html_get_prd_urls(
        rd = rd_lst[[i]],
        urls_in = urls[((i - 1) * width + 1):min(i * width, .N), URL],
        base_url = url_raw,
        sleep = 3
      )
      fwrite(
        prd_urls,
        file = file.path(out_path, paste0("prd_urls_async_core_", i, ".csv.gz"))
      )
    }
  }
}

amazon_process_prd_urls <- function (
  prd_path,
  url_path
) {
  out_path <- file.path(prd_path, "all")
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }  
  urls <- as.data.table(
    read.xlsx(
      file.path(url_path, "urls_levels.xlsx")
    )
  )
  levels <- as.numeric(
    gsub(
      "URL_",
      "",
      names(urls),
      fixed = TRUE
    )
  )
  if (max(levels) > 1) {
    for (l in 1:max(levels)) {
      if (l == max(levels)) {
        urls[
          !is.na(get(paste0("URL_", l))),
          DEPTH := l
        ]
      } else {
        urls[
          !is.na(get(paste0("URL_", l))) & is.na(get(paste0("URL_", l + 1))),
          DEPTH := l
        ]
      }
    }
  } else {
    urls[, DEPTH := 1]
  }
  for (l in levels) {
    urls[
      DEPTH == l,
      URL := get(
        paste0(
          "URL_",
          l
        )
      )
    ]
  }
  prd_urls <- rbindlist(
    lapply(
      c(
        list.files(
          file.path(
            prd_path,
            "sync"
          ),
          full.names = TRUE
        ),
        list.files(
          file.path(
            prd_path,
            "async"
          ),
          full.names = TRUE
        )
      ),
      fread
    )
  )
  prd_urls[
    ,
    `:=`(
      PRD_ID = sapply( # PRD_ID is unique but not the link because of ads
        PRD_URL,
        function (u) {
          first(
            unlist(
              strsplit(
                last(
                  unlist(
                    strsplit(
                      u,
                      "/dp/",
                      fixed = TRUE
                    )
                  )
                ),
                "/ref",
                fixed = TRUE
              )
            )
          )
        }
      ),
      PRD_NAME = sapply(
        PRD_URL,
        function (u) {
          first(
            unlist(
              strsplit(
                u,
                "/dp/",
                fixed = TRUE
              )
            )
          )
        }
      )
    )
  ][
    ,
    first(.SD),
    keyby = .(URL, PRD_ID)
  ][
    ,
    .SD[1:min(.N, 100), ],
    keyby = .(URL)
  ]
  prd_urls <- merge(
    urls,
    prd_urls,
    by = "URL",
    all.x = TRUE,
    allow.cartesian = TRUE
  )
  write.xlsx(
    prd_urls,
    file = file.path(out_path, paste0("prd_urls.xlsx")),
    overwrite = TRUE
  )
}

amazon_html_get_prd_chars <- function (rd, base_url, prd_urls) {
  for (r in seq_len(nrow(prd_urls))) {
    if (prd_urls[r, !is.na(PRD_URL)]) {
      rd$navigate(paste0(base_url, prd_urls[r, PRD_URL]))
      page <- read_html(unlist(rd$getPageSource()))
      res <- page %>%
        html_elements("#productDetails_db_sections") %>%
        html_elements("td") %>%
        html_elements("span") %>%
        html_text()
      prd_urls[
        r,
        PRD_CHAR := paste(
          c(
            grep("^\\d+ évaluation.*$", res, perl = TRUE, value = TRUE),
            grep("^[\\d,]+ (en){1} .+$", res, perl = TRUE, value = TRUE)
          ),
          collapse = "|"
        )
      ]
    }
  }
  prd_urls
}

amazon_get_prd_chars <- function (
  in_path,
  out_path,
  url_raw,
  rd_lst,
  version = "async"
) {
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }
  prd_urls <- as.data.table(
    read.xlsx(
      file.path(in_path, "prd_urls.xlsx")
    )
  )
  if (version == "sync") {
    prd_urls <- amazon_html_get_prd_chars(
      rd = rd_lst[[1]],
      base_url = url_raw,
      prd_urls = prd_urls
    )
    write.xlsx(
      prd_urls,
      file = file.path(out_path, "prd_char_sync.xlsx"),
      overwrite = TRUE
    )
    pid <- rsd$server$process$get_pid()
    system(paste0("Taskkill /F /T" ," /PID ", pid))
  } else {
    width <- ceiling(prd_urls[, .N / length(rd_lst)])
    registerDoParallel(cl <- makeCluster(length(rd_lst)))
    foreach(
      i = seq_len(length(rd_lst)),
      .packages = c("rvest", "RSelenium", "data.table", "stringr"),
      .export = c(
        "amazon_html_get_prd_chars"
      )
    ) %dopar% {
      prd_urls <- amazon_html_get_prd_chars(
        rd = rd_lst[[i]],
        base_url = url_raw,
        prd_urls = prd_urls[((i - 1) * width + 1):min(i * width, .N), ]
      )
      fwrite(
        prd_urls,
        file = file.path(out_path, paste0("prd_char_async_core_", i, ".csv.gz"))
      )
    }
  }
}

amazon_process_prd_chars <- function (
  prd_path,
  url_path
) {
  out_path <- file.path(
    prd_path,
    "all"
  )
  if (!dir.exists(out_path)) {
    dir.create(out_path, recursive = TRUE)
  }
  for (t in c("async", "sync")) {
    if (t == "async") {
      prd_char <- rbindlist(
        lapply(
          list.files(
            file.path(
              prd_path,
              t
            ),
            full.names = TRUE
          ),
          function (f) {
            fread(
              f,
              encoding = "UTF-8"
            )
          } 
        )
      )
    } else {
      prd_char <- as.data.table(
        read.xlsx(
          file.path(
            prd_path,
            t,
            paste0(
              "prd_char_",
              t,
              ".xlsx"
            )
          )
        )
      )
    }
    prd_char[
      ,
      `:=`(
        NB_ELT = sapply(
          PRD_CHAR,
          function (e) {
            length(
              unlist(
                strsplit(
                  e,
                  "|",
                  fixed = TRUE
                )
              )
            )
          }
        ),
        NB_CLS = sapply(
          gsub("évaluation.*\\|", "", PRD_CHAR, perl = TRUE),
          function (e) {
            length(
              unlist(
                strsplit(
                  e,
                  "|",
                  fixed = TRUE
                )
              )
            )
          }
        )
      )
    ]
    nb_max <- prd_char[, max(NB_ELT)]
    nb_cls_max <- prd_char[, max(NB_CLS)]
    prd_char[
      ,
      c("NB_EVAL", paste0("V_", 1:(nb_max - 1))) := tstrsplit(
        PRD_CHAR,
        "|",
        fixed = TRUE
      )
    ]
    prd_char[
      ,
      paste0("V_", 1:(nb_max - 1)) := NULL
    ]
    prd_char[
      !grepl("évaluation.*", NB_EVAL),
      NB_EVAL := NA
    ]
    prd_char[
      ,
      paste0("CLASSEMENT_", 1:nb_cls_max) := tstrsplit(
        gsub("évaluation.*\\|", "", PRD_CHAR, perl = TRUE),
        "|",
        fixed = TRUE
      )
    ]
    urls_labels <- as.data.table(
      read.xlsx(file.path(url_path, "urls_labels_async.xlsx"))
    )
    levels <- as.integer(
      gsub(
        "URL_",
        "",
        grep(
          "URL_\\d",
          names(prd_char),
          perl = TRUE,
          value = TRUE
        )
      )
    )
    for (l in 1:max(levels)) {
      prd_char <- setnames(
        merge(
          prd_char,
          urls_labels,
          by.x = paste0("URL_", l),
          by.y = "URL",
          all.x = TRUE
        ),
        "URL_LABEL",
        paste0("URL_LABEL_", l)
      )
    }
    setcolorder(
      prd_char,
      c(
        "DEPTH",
        paste0("URL_", levels),
        paste0("URL_LABEL_", levels),
        "PRD_ID", "PRD_URL", "PRD_CHAR", "NB_EVAL",
        grep(
          "CLASSEMENT_\\d",
          names(prd_char),
          perl = TRUE,
          value = TRUE
        )
      )
    )
    write.xlsx(
      prd_char,
      file = file.path(
        out_path,
        paste0("scraping_bestsellers_amazon_", t, ".xlsx")
      ),
      overwrite = TRUE
    )
  }
}
