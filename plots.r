## Wanna show


source("./model.r")
source("./aws.r")

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

## TODO:
## - show all, grey
## - pareto on top
##
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
             y = stat.price.sum,
             is.flank = x == 0 | id != dplyr::lag(id),
             color = ifelse(is.flank, "blue", "black"),
             label = str_replace(id, "xlarge", "")
           )
  .df.flanks <- .df %>% dplyr::filter(is.flank)
  ggplot(.df, aes(x = x, y = y)) +
    geom_line(color = "black") +
    geom_point(color = .df$color) +
    # geom_vline(data = .df.flanks, aes(xintercept=x)) +
    geom_text(aes(label = label), nudge_x = 17, nudge_y = 0.03, size = 2.2) +
    theme_light() +
    theme(text = element_text(size = 7), plot.margin=grid::unit(c(0,0,0,0), "mm")) +
    labs(x = "CPUh", y = "Workload Cost")
}

## ggsave(plots.mkpath("m1-cost-cpu.pdf"), plots.m1.draw(),
##        width = 3, height = 2, units = "in",
##        device = cairo_pdf)


## ---------------------------------------------------------------------------------------------- ##
                                        # M2: Caching #
## ---------------------------------------------------------------------------------------------- ##

## Config where changing only access distribution results in c5n -> c5d -> c5
## http://127.0.0.1:3030/?_inputs_&instances.slicing_search=%22%22&instances.times_cell_clicked=%7B%7D&timings.plot.budget.ticks.at.limits=true&timings.plot.budget.step.digits=3&instances.rws.gib_rows_current=%5B1%2C2%2C3%5D&instance=%22c5.24xlarge%22&instances.specs.derived_rows_all=%5B1%2C2%5D&instances.times.perc_rows_all=%5B1%2C2%2C3%5D&instances.slicing_rows_current=%5B1%2C2%5D&instances.slicing_cell_clicked=%7B%7D&instances.times_rows_all=%5B1%2C2%2C3%5D&instances.prices_rows_all=%5B1%2C2%2C3%5D&time.cpu=12&instances.plot.frontier.quadrant=%22top.left%22&instances.metadata_search=%22%22&instances.specs_rows_current=%5B1%2C2%5D&instances.reads.gib_state=null&instances.prices_search=%22%22&instances.times.perc_state=null&instances.reads.gib_rows_all=%5B1%2C2%2C3%5D&instFilter.details.show=0&config.options.show=1&instances.plot.display.frontier.only=false&time.period.num=1&instances.plot.x=%22stat.time.sum%22&instances.slicing_rows_selected=null&plotly_hover-A=null&instances.rws.gib_state=null&instances.specs.derived_cell_clicked=%7B%7D&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&scaling.efficiency.param.%20p=0.98&instances.plot.scale.y=%22Linear%22&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&instances.specs.derived_rows_current=%5B1%2C2%5D&instances.specs_rows_selected=null&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.specs_cell_clicked=%7B%7D&instances.rws.gib_search=%22%22&instances.metadata_rows_selected=null&instances.metadata_rows_current=%5B1%2C2%5D&instances.metadata_state=null&instances.prices_cell_clicked=%7B%7D&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&instances.specs.derived_search=%22%22&instances.specs_state=null&instances.rws.gib_rows_all=%5B1%2C2%2C3%5D&instances.times_rows_selected=null&distr.caching.load.first=false&instance.count.max=1&instances.specs_rows_all=%5B1%2C2%5D&instances.reads.gib_rows_current=%5B1%2C2%2C3%5D&locality=0.96&instances.specs_search=%22%22&instSet.details.show=0&instances.reads.gib_rows_selected=null&instanceFilter=%22Paper%20Table%201%22&instances.times_rows_current=%5B1%2C2%2C3%5D&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A400%7D%22&spooling.fraction=0&tables.frontier.show=false&instances.reads.gib_search=%22%22&instances.plot.display.frontier=true&instances.metadata_cell_clicked=%7B%7D&instances.times_search=%22%22&data.read=1798&instances.times.perc_rows_selected=null&instance.type.opt.include=%22frontier%22&time.cpu.maxval=1000&instances.slicing_state=null&instances.prices_state=null&spooling.shape=0.1&data.read.maxval=10000&instances.times_state=null&instances.prices_rows_current=%5B1%2C2%2C3%5D&instances.reads.gib_cell_clicked=%7B%7D&instances.rws.gib_cell_clicked=%7B%7D&recommendationColumn=%22stat.price.sum%22&instances.specs.derived_rows_selected=null&plotly_afterplot-A=%22%5C%22distr.spooling.recommended.plot%5C%22%22&instances.specs.derived_state=null&instances.rws.gib_rows_selected=null&instances.metadata_rows_all=%5B1%2C2%5D&instances.times.perc_cell_clicked=%7B%7D&instances.prices_rows_selected=null&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&instances.times.perc_search=%22%22&comparison.count=1&instances.times.perc_rows_current=%5B1%2C2%2C3%5D&instances.slicing_rows_all=%5B1%2C2%5D

plots.m2.cost.draw <- function() {
  .read <- 1800
  .query <- data.frame(
    time.cpu  = 12,
    data.read =  1800
  )
  .shapes <- c(0.001, seq(0.1, 0.9, 0.1), 0.999)
  .cost.list <- purrr::map_dfr(.shapes, function(.shape) {
    .distr <- model.make.distr.fn(.shape)(.read)
    .time.fn <- model.make.timing.fn(
      .distr.list.caching = list(.distr),
      .distr.list.spooling = list(0),
      .max.count = 1,
      .distr.caching.split.fn = model.distr.split.fn(FALSE)
    )
    .costs <- model.calc.costs(.query, plots.inst, .time.fn)
    .recom <- model.recommend.from.timings.arr(.query, .costs)
    head(.recom, n = 1)
  })
  .df <- .cost.list %>%
    dplyr::transmute(
             x = .shapes,
             y = stat.price.sum,
             is.flank = x == .shapes[1] | id != dplyr::lag(id),
             color = ifelse(is.flank, "blue", "black"),
             label = str_replace(id.name, "xlarge", "")
           )
  .df.flanks <- .df %>% dplyr::filter(is.flank)
  ggplot(.df, aes(x = x, y = y)) +
    geom_line(color = "black") +
    geom_point(color = .df$color) +
    geom_text(aes(label = label), nudge_x = 0.07, nudge_y = 0.02, size = 2.2, angle = 30) +
    theme_light() +
    theme(text = element_text(size = 7), plot.margin=grid::unit(c(0,0,0,0), "mm")) +
    labs(x = "Locality Distribution Factor", y = "Workload Cost")
}

## plots.m2.cost.draw()

## ggsave(plots.mkpath("m2-cost-zipf.pdf"), plots.m2.cost.draw(),
##        width = 3, height = 2, units = "in",
##        device = cairo_pdf)

plots.m2.distr.draw <- function() {
  instance <- plots.inst %>% dplyr::filter(id == "c5d.24xlarge") %>% model.with.speeds()
  dist <- model.make.distr.fn(0.2)(2000)

  data <- data.frame(y = dist, x = 1:length(dist))
  if (instance$calc.net.speed >= instance$calc.sto.speed) {
    data$group = if_else(data$x <= instance$calc.mem.caching, "Memory", "S3")
  } else {
    data$group = if_else(data$x <= instance$calc.mem.caching, "Memory",
                         if_else(data$x <= instance$calc.mem.caching + instance$calc.sto.caching, "SSD", "S3"))
  }
  nudge.x <- 0.03 * max(c(max(instance$calc.mem.caching + instance$calc.sto.spooling), nrow(data)))
  nudge.y <- max(data$y) / 4
  plot <- ggplot(instance) +
    scale_fill_manual(values=ui.styles.color.palette1) +
    geom_area(data=data, aes(y = y, x = x, fill = group), stat="identity") +
    geom_vline(aes(xintercept=calc.mem.caching), colour="blue") +
    geom_vline(aes(xintercept=calc.mem.caching + calc.sto.spooling), colour="blue") +
    geom_text(aes(x=calc.mem.caching, y=nudge.y * 2, label="Instance Memory"), colour="blue", angle=90,
              nudge_x = nudge.x, nudge_y = nudge.y, size = 2.2) +
    geom_text(aes(x=calc.mem.caching + calc.sto.caching, y=nudge.y * 2, label="Instance Storage"), colour="blue", angle=90,
              nudge_x = nudge.x, nudge_y = nudge.y, size = 2.2) +
    labs(x = "GiB in Workload", y = "Number of Accesses", fill = "Hierarchy") +
    theme_light() +
    theme(text = element_text(size = 7), plot.margin = grid::unit(c(0.5, 0, 0, 0), "mm"),
          legend.title = element_blank(), legend.text = element_text(size = 7),
          legend.position = "bottom", legend.key.size = unit(0.5, "lines"),
          legend.margin = margin(0, 0, 0, 0, "cm"))
  plot
}

## plots.m2.distr.draw()

## ggsave(plots.mkpath("m2-distr-zipf.pdf"), plots.m2.distr.draw(),
##        width = 3, height = 2, units = "in",
##        device = cairo_pdf)
