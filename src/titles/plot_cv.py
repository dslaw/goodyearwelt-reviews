import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


sns.set_style("darkgrid")

def read(filename):
    df = pd.read_csv(filename)
    df["split"] = df.train.map(lambda b: "train" if b else "test")
    return df


df = read("data/model_cv.csv")
gb = df.groupby("name")

fig, axes = plt.subplots(1, len(gb))
for ax, (name, df_g) in zip(axes, gb):
    ax = sns.lineplot(x="k", y="log_loss", hue="split", data=df_g, ax=ax)
    ax.legend(title=None)
    ax.set_title(name.title())

fig, axes = plt.subplots(1, len(gb))
for ax, (name, df_g) in zip(axes, gb):
    ax = sns.lineplot(x="k", y="precision", hue="split", data=df_g, ax=ax)
    ax.legend(title=None)
    ax.set_title(name.title())

fig, axes = plt.subplots(1, len(gb))
for ax, (name, df_g) in zip(axes, gb):
    ax = sns.lineplot(x="k", y="recall", hue="split", data=df_g, ax=ax)
    ax.legend(title=None)
    ax.set_title(name.title())
