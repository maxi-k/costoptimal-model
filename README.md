# Towards Cost-Optimal Query Processing in the Cloud - Model Implementation

The implementation for the cost model introduced in the paper "[Towards Cost-Optimal Query Processing in the Cloud](https://www.cs6.tf.fau.de/files/2021/04/costoptimal.pdf)".


## Running

This version of the model is implemented in *R*. 
As such you need to have R installed on your computer, see [here](https://www.r-project.org/).
We also depend on various R packages that are not part of the basic installation.
These should install automatically when running any file. 
If they don't, please look at the list in `util.r#util.packages.basic.setup` and install them manually.

To start an interactive, browser-based explorer for the model predictions, please execute the file `app.r` by following these steps:
1. Run `R` in a command line. This should open an `R` [REPL](https://en.wikipedia.org/wiki/REPL).
2. Type `source("app.r")` in the R REPL. This will load the source file and should open your browser with the URL <http://0.0.0.0:3030>, displaying the model explorer.

To generate the plots used in the paper, please execute the respective code blocks in the `plots.r` file.

## Project Structure

Files of interest:
| Path | Description 
|-|-
| aws.r | Code for loading and manipulating aws instance data from <https://ec2instances.info> 
| model.r | The main model implementation as described in the paper
| elasticity.r | The greedy auto-scaling algorithm and accompanying plots 
| app.r | The interactive web-based UI for exploring model predictions
| plots.r | `ggplot` sources for the plots shown in the paper

The main algorithms relating to the model section in the paper are implemented in the functions `model.calc.costs` and `model.calc.time.for.config` in `model.r`.

Note that this is exploratory code for research purposes; code quality and maintainability are secondary.
