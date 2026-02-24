import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    return mo, pd


@app.cell
def _(mo):
    mo.md(r"""
    # Merge author names into books parquet

    Reads `3_goodreads_books_with_metrics.parquet` and `raw_goodreads_book_authors.parquet`,
    extracts the primary author's name from the nested `authors` list, and saves a new parquet
    with an `author_name` column appended.
    """)
    return


@app.cell
def _(pd):
    books = pd.read_parquet("3_goodreads_books_with_metrics.parquet")
    author_lookup = pd.read_parquet("raw_goodreads_book_authors.parquet")
    print(f"books:         {len(books):,} rows, columns: {list(books.columns)}")
    print(f"author_lookup: {len(author_lookup):,} rows, columns: {list(author_lookup.columns)}")
    books.head(3)
    return author_lookup, books


@app.cell
def _(author_lookup, books):
    # Each cell in `authors` is a list of dicts like [{'author_id': '604031', 'role': ''}].
    # Take only the first (primary) author per book.
    # authors column is decoded as a numpy array of dicts by pandas
    primary_author_id = books["authors"].apply(
        lambda lst: lst[0]["author_id"] if len(lst) > 0 else None
    )

    # Build a simple id -> name map from the authors parquet.
    # author_id may be int or string depending on the parquet — normalise to str.
    id_to_name = author_lookup.set_index(
        author_lookup["author_id"].astype(str)
    )["name"]

    books_with_author = books.copy()
    books_with_author["author_name"] = primary_author_id.astype(str).map(id_to_name)

    missing = books_with_author["author_name"].isna().sum()
    print(f"Rows with author name resolved: {len(books_with_author) - missing:,}")
    print(f"Rows with no match in lookup:   {missing:,}")
    books_with_author[["title", "authors", "author_name"]].head(10)
    return books_with_author, missing


@app.cell
def _(missing, mo):
    mo.callout(
        mo.md(
            f"**{missing:,}** books had no matching author in `raw_goodreads_book_authors.parquet` "
            f"and will have `author_name = NaN`."
        ),
        kind="warn" if missing > 0 else "success",
    )
    return


@app.cell
def _(books_with_author):
    output_path = "3_goodreads_books_with_metrics_author_name.parquet"
    books_with_author.to_parquet(output_path, index=False)
    print(f"Saved → {output_path}  ({len(books_with_author):,} rows)")
    return


if __name__ == "__main__":
    app.run()
