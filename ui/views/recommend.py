"""Page 1: recommendation demo.

Pick a user, see their recent activity (user:recent) and current
recommendations with their source, then simulate a view / cart / purchase:
the event goes to Kafka, the streaming job updates user:recent and rec:rt in
the next micro-batch, and the page shows the new list next to the old one.
"""

from __future__ import annotations

import streamlit as st

from ui import helpers as h

st.title("Recommendations")
st.caption("Kafka → Spark Structured Streaming → Redis → FastAPI. "
           "Each recommendation says where it comes from: batch (ALS), realtime (rec:rt), both, trending or popular.")

status = h.stream_status()
clock = h.stream_clock()
c1, c2, c3 = st.columns(3)
c1.metric("Stream clock (newest event processed)", clock.strftime("%Y-%m-%d %H:%M:%S") if clock else "no data")
c2.metric("Events processed", f"{int(status.get('events_total_rows', 0)):,}")
c3.metric("Last micro-batch", f"{status.get('events_batch_rows', '-')} rows, {status.get('events_batch_ms', '-')} ms")

# ---- user picker ----------------------------------------------------------------
with st.sidebar:
    st.subheader("User")
    if "user_id" not in st.session_state:
        st.session_state.user_id = 516407514
    examples = h.sample_users()
    if examples:
        pick = st.selectbox("Users with real-time scores", ["(type one below)"] + examples)
        if pick != "(type one below)" and st.button("Use this user"):
            st.session_state.user_id = int(pick)
    st.number_input("User ID", min_value=1, step=1, key="user_id")
    k = st.slider("Number of recommendations", 1, 50, 10)
    st.caption(f"API {h.API_URL} · Kafka {h.KAFKA_BOOTSTRAP} · Redis {h.REDIS_HOST}:{h.REDIS_PORT}")

user_id = int(st.session_state.user_id)

left, right = st.columns([2, 3])

with left:
    st.subheader("Recent activity")
    try:
        activity = h.recent_activity(user_id)
    except Exception as exc:                      # Redis down: the page still shows the API part
        activity = None
        st.error(f"Redis unreachable: {exc}")
    if activity is not None and activity.empty:
        st.info("No recent events for this user (new or inactive user): the API falls back to trending.")
    elif activity is not None:
        st.dataframe(activity.drop(columns="epoch"), hide_index=True, width="stretch")

with right:
    st.subheader("Recommendations")
    data, ms, err = h.api_get(f"/recommend/{user_id}", k=k)
    if err:
        st.error(err)
    else:
        a, b, c = st.columns(3)
        a.metric("Batch (rec:batch)", "yes" if data["batch_available"] else "no")
        b.metric("Real-time (rec:rt)", "yes" if data["realtime_available"] else "no")
        c.metric("API latency", f"{ms:.1f} ms")
        current = h.items_frame(data["items"])
        before = st.session_state.get("before")
        if before is not None and before.get("user") == user_id:
            old_ids = set(before["ids"])
            current["new"] = ["new" if p not in old_ids else "" for p in current["product_id"]]
        st.dataframe(current, hide_index=True, width="stretch")

# ---- simulate an event ------------------------------------------------------------
st.divider()
st.subheader("Simulate an event")
st.caption("The event is stamped just after the stream clock (the replayed data is from 2019), "
           "sent to Kafka with key = user_id, and picked up by the next micro-batch.")

choices = []
if not err and data["items"]:
    choices += [int(i["product_id"]) for i in data["items"]]
if activity is not None and not activity.empty:
    choices += [int(p) for p in activity["product_id"] if int(p) not in choices]

s1, s2, s3 = st.columns([2, 1, 1])
with s1:
    picked = st.selectbox("Product: pick a recommended or recently seen one, or type any product ID and press Enter",
                          choices or [1005100], index=0, accept_new_options=True)
    product_id = int(str(picked).strip()) if str(picked).strip().isdigit() else None
    if product_id is None:
        st.error(f"Not a product ID: {picked!r}")
with s2:
    event_type = st.radio("Event type", ["view", "cart", "purchase"], horizontal=False)
with s3:
    st.write("")
    send = st.button(f"Send {event_type} of {product_id}" if product_id else "Send",
                     type="primary", width="stretch", disabled=product_id is None)

if send:
    st.session_state.before = {"user": user_id, "ids": [] if err else [i["product_id"] for i in data["items"]]}
    try:
        event = h.send_event(user_id, product_id, event_type)
    except Exception as exc:
        st.error(f"Could not publish the event: {exc}")
    else:
        with st.spinner("Waiting for the streaming job to process it..."):
            seconds = h.wait_for_event(user_id, event)
        if seconds is None:
            st.session_state.flash = ("warning", f"Sent {event.to_json()} but it was not processed within 30 s. "
                                      "Is the streaming container running (`dc logs streaming`)?")
        else:
            st.session_state.flash = ("success", f"{event.event_type} of {event.product_id} processed by the speed "
                                      f"layer in {seconds:.1f} s. Products that were not in the previous list "
                                      "are marked \"new\".")
        st.rerun()

flash = st.session_state.pop("flash", None)
if flash:
    getattr(st, flash[0])(flash[1])
