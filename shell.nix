{ pkgs ? import <nixpkgs> {} }:

with pkgs;

mkShell {
    buildInputs =
        let exec = [ R ];
            r = with pkgs.rPackages; [
                Cairo
                readr
                tidyverse
                caret
                wrapr
                shiny
                slider
                sqldf
                VGAM
                ggrepel
                plotly
                DT
                RColorBrewer
                lubridate
                memoise
                parallel
                furrr
                shades
                C50
                pls
                gbm
            ];
        in exec ++ r;
}
