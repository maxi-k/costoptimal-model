# Towards Cost-Optimal Query Processing in the Cloud - Model Implementation

The R implementation for the model described in the paper "Towards Cost-Optimal Query Processing in the Cloud".

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
