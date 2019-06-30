import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


sns.set_style("darkgrid")

def read(filename):
    df = pd.read_csv(filename)
    df["split"] = df.train.map(lambda b: "train" if b else "test")
    return df


df = read("data/model_cv.csv")
long = pd.melt(
    df,
    id_vars=["k", "name", "split"],
    value_vars=["precision", "recall"],
    value_name="measure",
    id_name="measure_type",
)

g = (
    sns.FacetGrid(long, col="variable", row="name", hue="split", sharey=False)
    .map(sns.lineplot, "k", "measure")
    .add_legend()
)
