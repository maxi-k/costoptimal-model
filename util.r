## ---------------------------------------------------------------------------------------------- ##
                                        # Packages
## ---------------------------------------------------------------------------------------------- ##

util.packages.install <- function(pkgs, mirror = "https://cran.uni-muenster.de/")  {
    pkgs.new <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
    if(length(pkgs.new)) install.packages(pkgs.new, repos=mirror)

    return(pkgs.new)
}

util.packages.require <- function(pkgs) {
    for (pkg in pkgs) {
        library(pkg, character.only = TRUE)
    }
}

util.packages.basic.setup <- function() {
    base <- c("readr", "tidyverse", "caret", "shiny", "slider", "sqldf",
              "VGAM", "ggrepel", "plotly", "DT", "Cairo", "RColorBrewer",
              "lubridate", "memoise", "parallel", "furrr", "shades")
    modeling <- c("C50", "pls", "gbm")
    util.packages.install(c(base, modeling))
    util.packages.require(base)
    return(base)
}

util.packages.basic.setup()

## ---------------------------------------------------------------------------------------------- ##
                                        # Font Setup
## ---------------------------------------------------------------------------------------------- ##

util.style.fonts.setup <- function() {
  mainfont <- "Garamond"
  CairoFonts(regular = paste(mainfont,"style=Regular",sep=":"),
             bold = paste(mainfont,"style=Bold",sep=":"),
             italic = paste(mainfont,"style=Italic",sep=":"),
             bolditalic = paste(mainfont,"style=Bold Italic,BoldItalic",sep=":"))
  pdf <- CairoPDF
  png <- CairoPNG
}

util.style.fonts.setup()

styles.draw.palette <- function(colors) {
  df <- data.frame(y = 1:length(colors), x = 1)
  ggplot(df, aes(x = x, y = y, fill = colors, label = colors)) +
    scale_fill_identity() +
    geom_tile() +
    geom_text() +
    scale_x_continuous(expand = c(0, 0)) +
    scale_x_continuous(expand = c(0, 0)) +
    theme(legend.position = "none")
}

## From https://github.com/arcticicestudio/nord
styles.color.palette1 <- c(
    "#a3be8c",
    "#b48ead",
    "#ebcb8b",
    "#8fbcbb",
    "#5e81ac",
    "#4c566a",
    "#bf616a",
    "#d08770",
    "#81a1c1",
    "#d8dee9",
    "#88c0d0"
)

styles.color.palette.light <- c(
    "#a3be8c",
    "#b48ead",
    "#ebcb8b",
    "#8fbcbb",
    "#5e81ac",
    "#bf616a",
    "#d08770",
    "#81a1c1",
    "#d8dee9",
    "#88c0d0"
)

styles.color.palette.temparature <- c(
  "1.0"  = "#64CF17",
  "1.0x" = "#64EE17",
  "1.1"  = "#76FF03",
  "1.2"  = "#C6FF00",
  "1.3"  = "#EEFF41",
  "1.4"  = "#F4FF81",
  "1.5"  = "#FFF176",
  "1.6"  = "#FFEB3B",
  "1.7"  = "#FBC02D",
  "1.8"  = "#FF8F00",
  "1.9"  = "#FF6F00",
  "2.0"  = "#E65100",
  "2.1"  = "#DF3D00",
  "2.2"  = "#DD2C00",
  "2.3"  = "#CF1C0C",
  "2.4"  = "#BE1C0F",
  "2.5"  = "#AD0D0D"
)

## ---------------------------------------------------------------------------------------------- ##
                                        # Other Utilities
## ---------------------------------------------------------------------------------------------- ##

util.write.vec.for.shell <- function(vec, file = "/tmp/r-data") {
    write(vec, file = file, ncolumns = 1, append = FALSE, sep = "\n")
}
