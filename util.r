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
    base <- c("readr", "tidyverse", "caret", "shiny", "slider",
              "VGAM", "ggrepel", "plotly", "DT", "Cairo",
              "lubridate", "memoise", "parallel", "furrr")
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


## ---------------------------------------------------------------------------------------------- ##
                                        # Other Utilities
## ---------------------------------------------------------------------------------------------- ##

util.write.vec.for.shell <- function(vec, file = "/tmp/r-data") {
    write(vec, file = file, ncolumns = 1, append = FALSE, sep = "\n")
}
