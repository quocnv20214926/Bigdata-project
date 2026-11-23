"""Streamlit demo of the recommender (Pair 2).

Pages:
  1. Recommendations     a user's recent activity and recommendations; simulate an
                         event and watch the recommendations change (week 6)
  2. Trending & similar  trending products by category, similar products (week 6)
  3. Evaluation          offline results and load test (week 8)

Run locally:  streamlit run ui/app.py   (from the repository root, PYTHONPATH=.)
In Docker:    http://localhost:8501
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:                     # `streamlit run ui/app.py` from the repo root
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

st.set_page_config(page_title="E-commerce Recommender", layout="wide")

pages = [
    st.Page("views/recommend.py", title="Recommendations", default=True),
    st.Page("views/explore.py", title="Trending & similar"),
    st.Page("views/evaluation.py", title="Evaluation"),
]
st.navigation(pages).run()
