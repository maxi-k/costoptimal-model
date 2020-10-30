source("./model.r")
source("./aws.r")
source("./param.finder.r")

plots.inst <- aws.data.paper
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

plots.m1.all.draw <- function() {
  .n.points <- 100
  .value.range <- seq(0, 60, length.out = .n.points)
  .xlim <- tail(.value.range, n = 1)
  .query <- data.frame(
    time.cpu  = .value.range,
    data.read = 3000
  )
  .ids <- c("c5n", "c5", "c5d", "m5dn", "i3", "i3en", "other")

  .inst <- aws.data.current.large.relevant

  palette <- style.instance.colors.vibrant
  palette["other"] <- "#cccccc"

  .costs <- model.calc.costs(.query, .inst, plots.m1.timing.fn)
  .df <- dplyr::group_modify(.costs, function(group, qid) {
    dplyr::transmute(group,
                     id = as.character(id),
                     x = .query$time.cpu[qid$queryIndex],
                     y = stat.price.sum,
                     label = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id),
                     is.best = y == min(y),
                     rank = rank(y),
                     color = ifelse(label %in% .ids, label, "other"))
  }) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(is.best, color) %>%
    dplyr::group_by(label) %>%
    dplyr::mutate(is.flank = rank != dplyr::lag(rank))

  .flanks <- .df %>% dplyr::filter(is.flank, is.best) %>% dplyr::ungroup()
  .transitions <- dplyr::filter(.flanks, x != min(x))
  .labeled <- .df %>%
    dplyr::filter(!any(is.best)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(color != "other", x == max(x))

  ggplot(.df, aes(x = x, y = y, group = label, color = color)) +
    scale_color_manual(values=palette) +
    geom_line(data = .df %>% dplyr::filter(color == "other")) +
    geom_line(data = .df %>% dplyr::filter(color != "other")) +
    theme_bw() +
    geom_point(data = .transitions, shape = 4, color = "black") +
    geom_vline(data = .transitions, aes(xintercept=x), color = "black", linetype = "dashed", size = 0.2) +
    geom_text(data = .flanks, aes(y = y, x = x + 0.42 * max(x), label = paste(label, "best"))) +
    geom_text(data = .labeled, aes(x = max(x) + 1, label = label), hjust = 0) +
    annotate(geom = "text", label = "x1e", x = 22.7, y = 14.3, color = "grey") +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm"),
          legend.position = "none") +
    coord_cartesian(xlim = c(1, 69), ylim = c(0, 15)) +
    labs(x = "CPU Hours", y = "Workload Cost ($)")
}

## plots.m1.all.draw()
## ggsave(plots.mkpath("m1-cost-cpu-all.pdf"), plots.m1.all.draw(),
##        width = 3.6, height = 1.9, units = "in",
##        device = cairo_pdf)


## ---------------------------------------------------------------------------------------------- ##
                                        # M2: Caching & Materialization #
## ---------------------------------------------------------------------------------------------- ##

## Config where changing only access distribution results in c5n -> c5d -> c5
## http://127.0.0.1:3030/?_inputs_&instances.slicing_search=%22%22&instances.times_cell_clicked=%7B%7D&timings.plot.budget.ticks.at.limits=true&timings.plot.budget.step.digits=3&instances.rws.gib_rows_current=%5B1%2C2%2C3%5D&instance=%22c5.24xlarge%22&instances.specs.derived_rows_all=%5B1%2C2%5D&instances.times.perc_rows_all=%5B1%2C2%2C3%5D&instances.slicing_rows_current=%5B1%2C2%5D&instances.slicing_cell_clicked=%7B%7D&instances.times_rows_all=%5B1%2C2%2C3%5D&instances.prices_rows_all=%5B1%2C2%2C3%5D&time.cpu=12&instances.plot.frontier.quadrant=%22top.left%22&instances.metadata_search=%22%22&instances.specs_rows_current=%5B1%2C2%5D&instances.reads.gib_state=null&instances.prices_search=%22%22&instances.times.perc_state=null&instances.reads.gib_rows_all=%5B1%2C2%2C3%5D&instFilter.details.show=0&config.options.show=1&instances.plot.display.frontier.only=false&time.period.num=1&instances.plot.x=%22stat.time.sum%22&instances.slicing_rows_selected=null&plotly_hover-A=null&instances.rws.gib_state=null&instances.specs.derived_cell_clicked=%7B%7D&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&scaling.efficiency.param.%20p=0.98&instances.plot.scale.y=%22Linear%22&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&instances.specs.derived_rows_current=%5B1%2C2%5D&instances.specs_rows_selected=null&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.specs_cell_clicked=%7B%7D&instances.rws.gib_search=%22%22&instances.metadata_rows_selected=null&instances.metadata_rows_current=%5B1%2C2%5D&instances.metadata_state=null&instances.prices_cell_clicked=%7B%7D&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&instances.specs.derived_search=%22%22&instances.specs_state=null&instances.rws.gib_rows_all=%5B1%2C2%2C3%5D&instances.times_rows_selected=null&distr.caching.load.first=false&instance.count.max=1&instances.specs_rows_all=%5B1%2C2%5D&instances.reads.gib_rows_current=%5B1%2C2%2C3%5D&locality=0.96&instances.specs_search=%22%22&instSet.details.show=0&instances.reads.gib_rows_selected=null&instanceFilter=%22Paper%20Table%201%22&instances.times_rows_current=%5B1%2C2%2C3%5D&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A400%7D%22&spooling.fraction=0&tables.frontier.show=false&instances.reads.gib_search=%22%22&instances.plot.display.frontier=true&instances.metadata_cell_clicked=%7B%7D&instances.times_search=%22%22&data.read=1798&instances.times.perc_rows_selected=null&instance.type.opt.include=%22frontier%22&time.cpu.maxval=1000&instances.slicing_state=null&instances.prices_state=null&spooling.shape=0.1&data.read.maxval=10000&instances.times_state=null&instances.prices_rows_current=%5B1%2C2%2C3%5D&instances.reads.gib_cell_clicked=%7B%7D&instances.rws.gib_cell_clicked=%7B%7D&recommendationColumn=%22stat.price.sum%22&instances.specs.derived_rows_selected=null&plotly_afterplot-A=%22%5C%22distr.spooling.recommended.plot%5C%22%22&instances.specs.derived_state=null&instances.rws.gib_rows_selected=null&instances.metadata_rows_all=%5B1%2C2%5D&instances.times.perc_cell_clicked=%7B%7D&instances.prices_rows_selected=null&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&instances.times.perc_search=%22%22&comparison.count=1&instances.times.perc_rows_current=%5B1%2C2%2C3%5D&instances.slicing_rows_all=%5B1%2C2%5D

## Config where chaning the materialization fraction changes from c5 -> c5d -> c5n
## http://127.0.0.1:3030/?_inputs_&instance.count.max=32&locality=0.8&timings.plot.budget.ticks.at.limits=true&instSet.details.show=0&instanceFilter=%22Paper%20Table%201%22&timings.plot.budget.step.digits=3&instance=%22%22&spooling.fraction=0.4&tables.frontier.show=false&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A9220%7D%22&instances.plot.display.frontier=true&time.cpu=20&instances.plot.frontier.quadrant=%22top.left%22&data.read=1998&instance.type.opt.include=%22frontier%22&instFilter.details.show=0&config.options.show=0&instances.plot.display.frontier.only=false&spooling.shape=0.1&instances.plot.x=%22stat.time.sum%22&time.period.num=1&plotly_hover-A=%22%5B%7B%5C%22curveNumber%5C%22%3A0%2C%5C%22pointNumber%5C%22%3A86%2C%5C%22x%5C%22%3A285.5405361662%2C%5C%22y%5C%22%3A0.28472599407325%7D%5D%22&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&recommendationColumn=%22stat.price.sum%22&instances.plot.scale.y=%22Linear%22&scaling.efficiency.param.%20p=0.98&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&plotly_afterplot-A=%22%5C%22instances.plot.queriesPerDollar%5C%22%22&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&comparison.count=1&distr.caching.load.first=false

all.params <- try.params(aws.data.current.large.relevant)

plots.m2.spool.draw <- function() {
  res <- all.params
  plotdata <- res %>%
    dplyr::filter(rank == 1) %>%
    dplyr::mutate(id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id))
  palette <- style.instance.colors
  ggplot(plotdata, aes(x = param.scanned, y = param.spool.frac,
                       label = paste(str_replace(id.name, "xlarge", ""),
                                     ifelse(meta.uses.spot.price, "-S", "-D"), sep=""))) +
    scale_fill_manual(values = palette) +
    geom_tile(aes(fill = id.prefix)) +
    geom_text() +
    scale_y_continuous(expand = c(0, 0),
                       breaks = seq(0, 1.0, 0.2),
                       labels = c("0", ".2", ".4", ".6", ".8", "1")) +
    scale_x_log10(expand = c(0, 0), breaks = c(100, 1024, 10 * 1024, 100 * 1024),
                  labels = c("100GB", "1TB", "10TB", "100TB")) +
    theme_bw() +
    theme(
      plot.margin=grid::unit(c(1,0,1,1), "mm"),
      legend.position = "none") +
    labs(
      x = "Scanned Data [log]",
      y = "Materialization Fraction") +
    facet_grid(cols = vars(param.cpuh), labeller = function(x) { "Best Instance" })
}

## plots.m2.spool.draw()
## ggsave(plots.mkpath("m2-spool-best.pdf"), plots.m2.spool.draw(),
##        width = 2.2, height = 2.5, units = "in",
##        device = cairo_pdf)

local({
  # all.params.max <- try.params(aws.data.current.large.relevant, by = stat.price.max)
  #
  .diff <- all.params %>%
    dplyr::group_by_at(vars(starts_with("param."))) %>%
    mutate(
      price.diff.absolute = stat.price.sum - min(stat.price.max),
      price.diff.fraction = stat.price.sum / min(stat.price.max),
      price.diff.fractext = sprintf("%.2f", price.diff.fraction),
      price.diff.fracdisc = ifelse(price.diff.fraction != 1 & price.diff.fraction < 1.1,
                                   "1.0x",
                                   sprintf("%.1f", pmin(2.5, price.diff.fraction)))
    ) %>% top_n(-1, wt = stat.price.sum)
  palette <- styles.color.palette.temperature
  texts <- purrr::map(styles.color.palette.temperature, function(c) {
    b <- shades::brightness(c)
    if(b > 0.85) { "#000000" } else { "#ffffff" }
  })
  plot <- ggplot(.diff, aes(x = param.scanned, y = param.spool.frac,
                    label = price.diff.fractext, fill = price.diff.fracdisc,
                    color = price.diff.fracdisc)) +
    scale_fill_manual(values = palette) +
    geom_tile() +
    scale_color_manual(values = texts) +
    geom_text(size = 2.3) +
    scale_y_continuous(expand = c(0, 0)) +
    scale_x_log10(expand = c(0, 0), breaks = c(100, 1024, 10 * 1024, 100 * 1024),
                  labels = c("100GB", "1TB", "10TB", "100TB")) +
    theme_bw() +
    theme(plot.title = element_text(size = 5, hjust = 0.5),
          legend.position = "none",
          plot.margin=grid::unit(c(1,1,1,0), "mm")) +
    labs(x = "Scanned Data [log]", y = "Materialization Fraction", title = "Price Sum / Price Max")
  ## ggsave(plots.mkpath("m2-spool-sum-max-diff.pdf"), plot,
  ##        width = 2.5, height = 2.5, units = "in",
  ##        device = cairo_pdf)
})

## plots.m2.spool.draw()
## ggsave(plots.mkpath("m2-spool-best.pdf"), plots.m2.spool.draw(),
##        width = 2.2, height = 2.5, units = "in",
##        device = cairo_pdf)

## ---------------------------------------------------------------------------------------------- ##

plots.m2.draw.diff.for <- function(.id) {
  .cost <- all.params %>% dplyr::group_by_at(vars(starts_with("param.")))

  .diff <- dplyr::group_modify(.cost, function(group, keys) {
    .best <- dplyr::filter(group, rank == 1)
    .self <-  dplyr::filter(group, id.name %in% .id)
    dplyr::mutate(.self,
                  id.short = str_replace(id.name, "xlarge", ""),
                  id.title = paste(sub("^([A-Za-z1-9-]+)\\..*", "\\1", id.name), "versus best"),
                  price.diff.absolute = stat.price.sum - .best$stat.price.sum,
                  price.diff.fraction = stat.price.sum / .best$stat.price.sum,
                  price.diff.fractext = ifelse(price.diff.fraction != 1 & price.diff.fraction < 1.1,
                                              "≈1",
                                              sprintf("%.1f", price.diff.fraction)),
                  price.diff.fracdisc = ifelse(price.diff.fraction != 1 & price.diff.fraction < 1.1,
                                                "1.0x",
                                               sprintf("%.1f", pmin(2.5, price.diff.fraction))))
  })

  id.levels <- paste(sub("^([A-Za-z1-9-]+)\\..*", "\\1", .id), "versus best")
  .diff$id.factor <- factor(.diff$id.title, levels = id.levels)

  palette <- styles.color.palette.temperature
  texts <- purrr::map(styles.color.palette.temperature, function(c) {
    b <- shades::brightness(c)
    if(b > 0.85) { "#000000" } else { "#ffffff" }
  })

  ggplot(.diff, aes(x = param.scanned, y = param.spool.frac,
                    label = price.diff.fractext, fill = price.diff.fracdisc,
                    color = price.diff.fracdisc)) +
    scale_fill_manual(values = palette) +
    geom_tile() +
    scale_color_manual(values = texts) +
    geom_text(size = 2.5) +
    scale_y_continuous(expand = c(0, 0)) +
    scale_x_log10(expand = c(0, 0), breaks = c(100, 1024, 10 * 1024, 100 * 1024),
                  labels = c("100GB", "1TB", "10TB", "100TB")) +
    theme_bw() +
    theme(legend.position = "none",
          axis.title.y = element_blank(),
          axis.text.y  = element_blank(),
          axis.ticks.y = element_blank(),
          axis.title.x = element_text(hjust = 0.3475),
          plot.margin=grid::unit(c(1,1,1,0), "mm")) +
    labs(x = "Scanned Data [log]", y = "Materialization Fraction") +
    facet_grid(cols = vars(id.factor))
}

## plots.m2.diff.inst <- c("c5.24xlarge", "c5d.24xlarge", "i3.16xlarge","c5n.18xlarge")
## plots.m2.draw.diff.for(plots.m2.diff.inst)
## ggsave(plots.mkpath("m2-spool-diff.pdf"), plots.m2.draw.diff.for(plots.m2.diff.inst),
##        width = 3 * 2.5, height = 2.5, units = "in",
##        device = cairo_pdf)


plots.m2.distr.caching <- function(instance, dist, labs.x = "Scanned Data (TB)", labs.y = "Accesses") {
  levels = c("Memory", "Storage", "S3", "S3 Initial Load")
  initdata <- dist$initial %>% data.frame(y = ., x = 1:length(.), group = "S3 Initial Load")
  data <- dist$working %>% data.frame(y = . + initdata$y, x = 1:length(.))
  data$group = if_else(data$x <= instance$calc.mem.caching + instance$calc.sto.caching, "Storage", "S3")
  data$group = if_else(data$x <= instance$calc.mem.caching, "Memory", data$group)
  nudge.x <- 0.01 * max(c(max(instance$calc.mem.caching + instance$calc.sto.caching), nrow(data)))
  nudge.y <- max(data$y) * 0.8
  line.size <- 0.3
  .mk.txt <- function(name) paste(instance$id.prefix, name, "capacity")
  plot <- ggplot(instance) +
    scale_fill_manual(values=styles.color.palette1) +
    geom_area(data=data, aes(y = y, x = x, fill = group), stat="identity") +
    geom_area(data=initdata, aes(y = y, x = x, fill = group), stat="identity") +
    geom_vline(aes(xintercept=calc.mem.caching), colour="blue", size = line.size) +
    geom_vline(aes(xintercept=calc.mem.caching + calc.sto.caching), colour="blue", size = line.size) +
    geom_text(aes(x=calc.mem.caching, y=nudge.y, label=.mk.txt("RAM")), colour="blue", nudge_x = nudge.x, hjust = 0, size = 3) +
    geom_text(aes(x=calc.mem.caching + calc.sto.caching, y=nudge.y, label=.mk.txt("SSD")), colour="blue", nudge_x = nudge.x, hjust = 0, size = 3) +
    geom_point(aes(x = 0, y = max(data$y)), shape = 4, color = styles.color.palette1[1]) +
    labs(x = labs.x, y = labs.y, fill = "Load Type") +
    coord_cartesian(expand = FALSE) +
    scale_x_continuous(breaks = seq(0, nrow(data), 1000), labels = seq(0, nrow(data) / 1000)) +
    scale_y_continuous(breaks = seq(0, max(data$y), 1)) +
    theme_bw() +
    theme(plot.margin=grid::unit(rep(0.8, 4), "mm"),
          axis.title = element_text(size = 9),
          legend.position = "none")
  plot
}

cache.distr.packed.reads <- function(.distr, .inst) {
  .bins <- list(
    data.mem = list(size = round(.inst$calc.mem.caching), prio = .inst$calc.mem.speed),
    data.sto = list(size = round(.inst$calc.sto.caching), prio = .inst$calc.sto.speed),
    data.s3  = list(size = rep(length(.distr$working), times = nrow(.inst)),
                    prio = .inst$calc.net.speed)
  )
  model.distr.pack(.bins, .distr$working) %>% as.data.frame()
}
cache.distr.plot.with <- function(skew, .inst = "r5d.24xlarge", .read = 8000, .split = FALSE) {
  cache.distr.inst <- aws.data.current.large %>% dplyr::filter(id == .inst) %>% model.with.speeds()
  cache.distr.read <- .read
  .distr <- model.distr.split.fn(.split)(model.make.distr.fn(skew)(cache.distr.read))
  print(cache.distr.packed.reads(.distr, cache.distr.inst))
  plots.m2.distr.caching(cache.distr.inst, .distr)

}

ggsave(plots.mkpath("m2-cache-distr.pdf"), cache.distr.plot.with(0.2, .read = 3000, .inst = "c5d.24xlarge"),
       width = 3.6, height = 1, units = "in",
       device = cairo_pdf)

## ---------------------------------------------------------------------------------------------- ##
                                        # M3: Scale Out & Down #
## ---------------------------------------------------------------------------------------------- ##

## Interesting Budget Configuration c5d -> c5n -> c5
## http://127.0.0.1:3030/?_inputs_&instance.count.max=32&locality=0.2&timings.plot.budget.ticks.at.limits=true&instSet.details.show=0&instanceFilter=%22Paper%20Table%201%22&timings.plot.budget.step.digits=3&instance=%22c5d.24xlarge%22&spooling.fraction=0.4&tables.frontier.show=false&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A9600%7D%22&instances.plot.display.frontier=true&time.cpu=25&instances.plot.frontier.quadrant=%22top.left%22&data.read=2008&plotly_doubleclick-A=%22%5C%22instances.plot%5C%22%22&instance.type.opt.include=%22all%22&plotly_click-A=%22%5B%7B%5C%22curveNumber%5C%22%3A0%2C%5C%22pointNumber%5C%22%3A4%2C%5C%22x%5C%22%3A2383.80703584345%2C%5C%22y%5C%22%3A0.116526956083714%7D%5D%22&instFilter.details.show=0&config.options.show=0&instances.plot.display.frontier.only=false&spooling.shape=0.1&instances.plot.x=%22stat.time.sum%22&time.period.num=1&plotly_hover-A=null&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&recommendationColumn=%22stat.price.sum%22&instances.plot.scale.y=%22Linear%22&scaling.efficiency.param.%20p=0.98&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&plotly_afterplot-A=%22%5C%22instances.plot.queriesPerDollar%5C%22%22&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&comparison.count=1&distr.caching.load.first=false

plots.m3.time.cost.draw <- function() {
  .wl1 <- data.frame(
    cpu.hours  = 5,
    data.scan  = 10000,
    max.count  = 128,
    cache.skew = 0.001,
    spool.skew = 0.001,
    spool.frac = 0.3,
    scale.fact = 0.95,
    load.first = FALSE,
    max.period = 2^30)
  #
  .def <- model.gen.workload(.wl1)
  .query <- .def$query
  .time.fn <- .def$time.fn

  ## .inst <- rbind(
  ##   aws.data.current.large.relevant,
  ##   aws.data.current %>% dplyr::filter(id == "c5d.2xlarge") %>% dplyr::mutate(id = "snowflake")
  ## )
  .inst <- aws.data.current.large.relevant.spot.lt5
  .palette <- style.instance.colors

  .cost.all <- model.calc.costs(.query, .inst, .time.fn) %>%
    dplyr::inner_join(select(.inst, id, starts_with("meta.")), by = c("id.name" = "id"))
  .frontier <- model.calc.frontier(.cost.all,
                                   x = "stat.time.sum", y = "stat.price.sum",
                                   id = "id", quadrant = "bottom.left")
  .df <- .cost.all %>% dplyr::mutate(
                                id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id.name),
                                group = ifelse(id %in% .frontier$id | id.prefix %in% names(.palette), id.prefix, "other"),
                                name = paste(group, ifelse(meta.uses.spot.price, "-S", "-D"), sep=""))

  .groups <- unique(.df$group)

  .palette["other"] <- "#eeeeee"
  .palette["frontier"] <- "#ff0000"

  .colored <- .df %>% dplyr::filter(group != "other")
  .greys <- .df %>% dplyr::filter(group == "other")

  .labels <- .colored %>%
    dplyr::group_by(group) %>%
    dplyr::filter(stat.price.sum == max(stat.price.sum))

  ggplot(.df, aes(x = stat.time.sum, y = stat.price.sum, color = group, label = name)) +
    scale_color_manual(values = .palette) +
    geom_point(data = .greys, size = 1) +
    geom_point(data = .colored, size = 1) +
    geom_text(data = .labels, nudge_x = -0.15, nudge_y = 0.08) +
    ## geom_text(data = .colored, aes(label = count)) +
    scale_x_log10(limits = c(30, 6e03)) +
    scale_y_log10() +
    labs(y = "Workload Cost ($) [log]",
         x = "Workload Execution Time (s) [log]",
         color = "Instance") +
    theme_bw() +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm"),
          legend.position = "none")
}

## plots.m3.time.cost.draw()
## ggsave(plots.mkpath("m3-time-cost.pdf"), plots.m3.time.cost.draw(),
##        width = 3.6, height = 2.6, units = "in",
##        device = cairo_pdf)

## ---------------------------------------------------------------------------------------------- ##
                                        # MH: History #
## ---------------------------------------------------------------------------------------------- ##

history.mkdata <- memoize(function() {
  .wl1 <- data.frame(
    cpu.hours  = 5,
    data.scan  = 10000,
    max.count  = 1,
    cache.skew = 0.001,
    spool.skew = 0.001,
    spool.frac = 0.3,
    scale.fact = 0.95,
    load.first = FALSE,
    max.period = 2^30)
  .wl2 <- dplyr::mutate(.wl1, data.scan = data.scan * 100)
  .wl3 <- dplyr::mutate(.wl1, cpu.hours = cpu.hours * 100000)
  #
  .def1 <- model.gen.workload(.wl1)
  .def2 <- model.gen.workload(.wl2)
  .def3 <- model.gen.workload(.wl3)
  #
  .filtered <- c("m6g")
  .inst <- aws.data.all.by.date %>% dplyr::filter(!(id.prefix %in% .filtered))
  .cost <- dplyr::group_modify(.inst, function(.slice, date) {
    .large <- aws.data.filter.large(.slice)
    .recom <- rbind(
      model.calc.costs(.def1$query, .large, .def1$time.fn) %>%
      model.recommend.from.timings.arr(.def1$query, .) %>%
      dplyr::mutate(workload.id = "A"),
      #
      model.calc.costs(.def2$query, .large, .def2$time.fn) %>%
      model.recommend.from.timings.arr(.def2$query, .) %>%
      dplyr::mutate(workload.id = "B"),
      #
      model.calc.costs(.def3$query, .large, .def3$time.fn) %>%
      model.recommend.from.timings.arr(.def3$query, .) %>%
      dplyr::mutate(workload.id = "C")
    )
    dplyr::inner_join(.large, .recom, by = c("id" = "id.name")) %>%
      dplyr::mutate(configuration = id.y)
  })
  #
  .cost %>%
    dplyr::mutate(label = paste(count, "×", str_replace(id, "xlarge", ""), sep=" "))
})

plots.mh.history.cost.draw <- function() {
  .df <- history.mkdata() %>%
    dplyr::group_by(workload.id) %>%
    dplyr::group_modify(function(group, key) {
      .first <- dplyr::filter(group, commit.date == min(commit.date))
      dplyr::mutate(group,
                    stat.price.change = stat.price.sum / .first$stat.price.sum
                    )
    })
  ggplot(.df, aes(x = meta.join.time, y = stat.price.change, label = label, color = workload.id)) +
    geom_line(aes(group = workload.id), linetype = "dashed") +
    ## geom_text(data = dplyr::filter(.df, workload.id == "A"), nudge_y = 0.01, nudge_x = 0.2) +
    geom_line(aes(group = paste(workload.id, id)), size = 1.5) +
    labs(x = "Date", y = "Normalized Workload Cost", color = "Workload") +
    theme_bw() +
    scale_y_continuous(limits = c(0, 1.1),
                       breaks = seq(0, 1, 0.2),
                       labels = c("0", ".2", ".4", ".6", ".8", "1")) +
    theme(legend.position = "none",
          plot.margin=grid::unit(c(1,1,1,1), "mm"))
}

## plots.mh.history.cost.draw()
## ggsave(plots.mkpath("mh-date-cost.pdf"), plots.mh.history.cost.draw(),
##        width = 3.6, height = 2.3, units = "in",
##        device = cairo_pdf)

## ---------------------------------------------------------------------------------------------- ##

spot.prices <- aws.data.spot.by.date.az %>%
  aws.data.filter.large() %>%
  aws.data.filter.relevant.family() %>%
  dplyr::filter(!(id.prefix %in% c("t2", "t3", "c4", "i2", "t3a", "inf1", "m3", "f1", "g2", "g4dn")))

plots.mh.spot.prices.draw <- function() {
  ggplot(spot.prices, aes(x = parsed.date)) +
    geom_line(aes(y = SpotPrice), color = "red") +
    expand_limits(y = 0) +
    facet_grid(rows = vars(id.prefix), cols = vars(AvailabilityZone), scales = "free")
}

## plots.mh.spot.prices.draw()

## ---------------------------------------------------------------------------------------------- ##

plots.mh.spot.gen.data <- memoize(function(.def, .freqs) {
  .wl <- model.gen.workload(.def)
  .freq <- aws.spot.interruption.frequencies %>%
    dplyr::filter(region.id == "us-east-1") %>%
    dplyr::group_by(instance.type) %>%
    dplyr::summarise(meta.freq.avg = mean(freq.num), .groups = "drop")
  #
  .inst <- aws.data.spot.joined %>%
    aws.data.filter.large2() %>%
    aws.data.filter.relevant.family2() %>%
    dplyr::filter(lubridate::month(parsed.date) == 8) %>%
    inner_join(.freq, by = c("id" = "instance.type"))
  #
  .inst.for.freq <- function(.freq) {
    .spot.priced <- .inst %>%
      dplyr::filter(meta.freq.avg <= .freq) %>%
      dplyr::mutate(cost.usdph = SpotPrice)
    rbind(
      dplyr::anti_join(.inst, .spot.priced, by = "id") %>% dplyr::mutate(meta.uses.spot.price = FALSE),
      .spot.priced %>% dplyr::mutate(meta.uses.spot.price = TRUE)
    )
  }
  #
  .recoms <- furrr::future_map_dfr(.freqs, function(.freq) {
    .inst.freq <- .inst.for.freq(.freq) %>% group_by(parsed.date)
    group_split(.inst.freq) %>% furrr::future_map_dfr(function(.inst.group) {
      .cost <- model.calc.costs(.wl$query, .inst.group, .wl$time.fn)
      .recm <- model.recommend.from.timings.arr(.wl$query, .cost) %>%
        dplyr::mutate(group = paste("<", .freq, "%", sep=""),
                      parsed.date = head(.inst.group, n = 1)$parsed.date)
    })
  })
  .recoms
})

plots.mh.spot.cost.draw <- function() {
  .recoms <- plots.mh.spot.gen.data(
    data.frame(
      cpu.hours  = 1,
      data.scan  = 4 * 10^3,
      max.count  = 1,
      cache.skew = 0.001,
      spool.skew = 0.001,
      spool.frac = 0.2,
      scale.fact = 0.95,
      load.first = FALSE,
      max.period = 2^30
    ),
    c(0, 5, 25)
  )

  .df <- .recoms %>%
    dplyr::mutate(id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id.name),
                  id.short  = str_replace(id.name, "xlarge", "")) %>%
    dplyr::ungroup()

  .text <- .df %>%
    dplyr::arrange(parsed.date) %>%
    dplyr::group_by(group) %>%
    dplyr::filter(row_number() == 1 | id != dplyr::lag(id)) %>%
    dplyr::ungroup()

  .vcenter = median(.df$parsed.date)

  od <- mean(filter(.df, group == "<0%")$stat.price.sum)
  c5 <- mean(filter(.df, group == "<5%")$stat.price.sum)
  c20 <- mean(filter(.df, group == "<25%")$stat.price.sum)
  print(c("<%5", c5, od, c5 / od))
  print(c("<%25", c20, od, c20 / od))

  plot <- ggplot(.df, aes(x = parsed.date,
                          y = stat.price.sum,
                          label = group, group = group, color = id.prefix)) +
    scale_color_manual(values = style.instance.colors.vibrant) +
    geom_line() +
    # geom_text(data = .text, angle = 90, nudge_y = 0.01, hjust = 0) +
    scale_y_continuous(limits = c(0, 0.685), breaks = seq(0, 1, 0.1)) +
    annotate(geom = "text", x = .vcenter, y = 0.645, label = "On Demand: i3 is best", color = style.instance.colors.vibrant["i3"]) +
    annotate(geom = "text", x = .vcenter, y = 0.335, label = "< 5% interruptions: m5n is best", color = style.instance.colors.vibrant["m5n"]) +
    annotate(geom = "text", x = .vcenter, y = 0.135, label = "> 20% interruptions: i3 is best", color = style.instance.colors.vibrant["i3"]) +
    labs(y = "Workload Cost ($)", x = "Date") +
    theme_bw() +
    theme(legend.position = "none",
          plot.margin=grid::unit(c(1,1,1,1), "mm"))
  plot
}

## plots.mh.spot.cost.draw()
## ggsave(plots.mkpath("mh-spot-prices.pdf"), plots.mh.spot.cost.draw(),
##        width = 3.6, height = 1.9, units = "in",
##        device = cairo_pdf)
## util.notify()
