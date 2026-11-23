"""Page 2: trending products (last hour, by category) and similar products."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ui import helpers as h

st.title("Trending & similar products")

k = st.sidebar.slider("Number of products", 1, 50, 10, key="explore_k")

tab_trend, tab_similar = st.tabs(["Trending (1 hour window)", "Similar products"])

with tab_trend:
    st.caption("`trend:global` / `trend:cat:{category}` from the speed layer (weighted counts view 1, cart 3, "
               "purchase 5 over the last hour of event time, sliding every 5 minutes). "
               "When a category had no activity in the last hour the API falls back to long-term popularity "
               "(`pop:*`, Pair 1).")
    glob, ms, err = h.api_get("/trending", k=50)
    if err:
        st.error(err)
        glob = {"items": []}
    # Categories present in the global list, plus any category id typed by hand.
    cats: dict[str, int] = {}
    for item in glob["items"]:
        if item.get("category_id"):
            label = f"{item.get('category_code') or 'unnamed'} ({item['category_id']})"
            cats.setdefault(label, int(item["category_id"]))
    col1, col2 = st.columns([2, 1])
    with col1:
        choice = st.selectbox("Category", ["All categories"] + list(cats))
    with col2:
        typed = st.text_input("or category ID", "")
    category = int(typed) if typed.strip().isdigit() else cats.get(choice)

    if category is None:
        data = {"items": glob["items"][:k]}
        latency = ms
    else:
        data, latency, err = h.api_get(f"/trending/{category}", k=k)
    if not err and data is not None:
        items = data["items"]
        source = items[0]["source"] if items else "-"
        st.caption(f"{len(items)} products · source: {source} · API {latency:.1f} ms")
        if items:
            df = h.items_frame(items).rename(columns={"score": "weighted count"})
            chart = df.assign(product=df["product_id"].astype(str))
            st.bar_chart(chart, x="product", y="weighted count", horizontal=True, sort="-weighted count")
            st.dataframe(df, hide_index=True, width="stretch")
        else:
            st.info("Nothing trending: the stream has not processed events for this category in the last hour.")

with tab_similar:
    st.caption("`sim:cooc` (speed layer: products viewed in the same session, normalised co-occurrence) and "
               "sim:item (batch layer: Item-kNN cosine). With both, the API returns a hybrid list.")
    default = glob["items"][0]["product_id"] if glob.get("items") else 1005100
    pid = st.number_input("Product ID", min_value=1, value=int(default), step=1)
    data, ms, err = h.api_get(f"/similar/{int(pid)}", k=k)
    attrs = h.product_attrs([pid]).get(int(pid), {})
    if attrs:
        st.write(f"**{pid}**: {h.category_label(attrs.get('category_code'), attrs.get('category_id'))}, "
                 f"{attrs.get('brand') or 'no brand'}, {attrs.get('price')}")
    if err:
        st.error(err)
    elif not data["items"]:
        st.info("No similar products yet (no co-views in the stream and no sim:item list).")
    else:
        st.caption(f"similarity = {data['similarity']} · API {ms:.1f} ms")
        df = h.items_frame(data["items"]).rename(columns={"score": "similarity"})
        df["source"] = df["source"].map({"realtime": "sim:cooc", "batch": "sim:item", "both": "both"}).fillna(df["source"])
        st.dataframe(df, hide_index=True, width="stretch")
        if attrs.get("category_id"):
            cats_sim = pd.Series([i.get("category_id") for i in data["items"]])
            share = (cats_sim == int(attrs["category_id"])).mean()
            st.caption(f"{share:.0%} of the similar products are in the same category.")
