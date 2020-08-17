source("./model.r")
source("./aws.r")

try.params <- memoize(function() {

  .range.scan  <- 16*2^(0:14) # ?
  .range.cache <- c(0.001)
  .range.sdist <- c(0.001)
  .range.spool <- seq(0, 1, by = 0.1)
  .range.cpu   <- c(1)
  .range.split <- c(FALSE)

  .insts <- aws.data.current.large.relevant

  purrr::map_dfr(.range.scan, function(.scanned) {
    purrr::map_dfr(.range.cache, function(.cache.skew) {
      .cache.distr <- model.make.distr.fn(.cache.skew)(round(.scanned))
      .cache.distr.list <- list(.cache.distr)
      purrr::map_dfr(.range.sdist, function(.spool.skew) {
        purrr::map_dfr(.range.spool, function(.spool.frac) {
          .spooled <- round(.spool.frac * .scanned)
          .spool.distr <- if(.spooled < 1) { 0 } else { model.make.distr.fn(.spool.skew)(.spooled) }
          .spool.distr.list <- list(.spool.distr)
          purrr::map_dfr(.range.cpu, function(.cpuh) {
            purrr::map_dfr(.range.split, function(.do.split) {
              .split.fn <-
                .query <- data.frame(
                  time.cpu  = .cpuh,
                  data.read = .scanned
                )
              .timer <- model.make.timing.fn(
                .distr.list.caching  = .cache.distr.list,
                .distr.list.spooling = .spool.distr.list,
                .max.count <- 1,
                .distr.caching.split.fn = model.distr.split.fn(.do.split)
              )
              .times <- model.calc.costs(.query, .insts, timing.fn = .timer)
              .recom <- .times %>%
                dplyr::mutate(rank = rank(stat.price.sum)) %>%
                dplyr::arrange(rank) %>%
                dplyr::ungroup()
              dplyr::mutate(.recom,
                            param.cpuh = .cpuh,
                            param.scanned = .scanned,
                            param.cache.skew = .cache.skew,
                            param.spool.skew = .spool.skew,
                            param.spool.frac = .spool.frac,
                            param.do.split   = .do.split
                            )
            })
          })
        })
      })
    })
  })
})

##  system.time({
##    tested.params <- try.params()
##    write.csv(res, "tested.params.with.split.csv")
##  })
