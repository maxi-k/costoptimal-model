source("./model.r")
source("./aws.r")

paper.inst.ids <- c(
 "c5n.18xlarge", "c5.24xlarge", "z1d.12xlarge", "c5d.24xlarge",
 "m5.24xlarge", "i3.16xlarge", "m5d.24xlarge", "m5n.24xlarge",
 "r5.24xlarge", "m5dn.24xlarge", "r5d.24xlarge", "r5n.24xlarge",
 "r5dn.24xlarge", "i3en.24xlarge", "x1e.32xlarge"
)

plots.inst <- aws.data.current %>% dplyr::filter(id %in% paper.inst.ids)
plots.path <- "../figures"

plots.mkpath <- function(filename) {
  paste(plots.path, filename, sep="/")
}

## ---------------------------------------------------------------------------------------------- ##
                                        # M1: Basic Model #
## ---------------------------------------------------------------------------------------------- ##

plots.m1.timing.fn <- function(.query, .inst) {
  dplyr::mutate(.inst,
                time.cpu.s = .query$time.cpu  * 3600 / calc.cpu.real,
                time.net.s = .query$data.read / calc.net.speed,
                stat.time.sum = time.cpu.s + time.net.s
                )
}

plots.m1.draw <- function() {
  .n.points <- 10
  .query <- data.frame(
    time.cpu  = seq(0, 200, length.out = .n.points),
    data.read =  rep(10000, .n.points)
  )
  .costs <- model.calc.costs(.query, plots.inst, plots.m1.timing.fn)
  .recom <- model.recommend.from.timings.arr(.query, .costs)
  .df <- .recom %>%
    dplyr::transmute(
             x = .query$time.cpu,
             y = 1 / stat.price.sum,
             is.flank = x == 0 | id != dplyr::lag(id),
             color = ifelse(is.flank, "blue", "black"),
             label = str_replace(id, "xlarge", "")
           )
  .df.flanks <- .df %>% dplyr::filter(is.flank)
  ggplot(.df, aes(x = x, y = y)) +
    geom_line(color = "black") +
    geom_point(color = .df$color) +
    # geom_vline(data = .df.flanks, aes(xintercept=x)) +
    geom_text(aes(label = label), nudge_x = 12, nudge_y = 0.03, size = 2.2) +
    theme_light() +
    theme(text = element_text(size = 9), plot.margin=grid::unit(c(0,0,0,0), "mm")) +
    labs(x = "CPUh", y = "Queries per Dollar")
}

ggsave(plots.mkpath("m1-cpu.pdf"), plots.m1.draw(),
       width = 9, height = 9, units = "cm",
       device = cairo_pdf)
