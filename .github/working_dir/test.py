import daft

df = daft.from_pydict({"a": [0, 1, 2]})
df = df.with_column("rooted", daft.col("a").sqrt()).collect()
df.show()
