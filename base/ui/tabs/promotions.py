"""🏷️ Promotions tab.

Weekly ads, coupons, and combo deals (grocery only).

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
from datetime import datetime, date, timedelta
import streamlit as st

from ui_lib.api import api_delete, api_get, api_patch, api_post


def render(ctx):
    @st.fragment
    def _promotions():
        if industry != "grocery":
            st.info("Promotions management is only available for the grocery industry.")
        else:
            st.subheader("Promotions")

            promo_tab_coupons, promo_tab_ads = st.tabs(["🎟️ Coupons", "📰 Weekly Ads"])

            # ── Coupons ──────────────────────────────────────────────────────────
            with promo_tab_coupons:
                st.markdown("#### Active Coupons")
                coupons_all = api_get("/grocery/pos/coupons", {"active_only": False, "limit": 500}) or []

                if coupons_all:
                    for coup in coupons_all:
                        with st.container(border=True):
                            cc1, cc2, cc3, cc4, cc5 = st.columns([2, 2, 1, 2, 1])
                            cc1.markdown(f"**{coup['code']}** — {coup['description']}")
                            cc2.write(f"{coup['coupon_type'].replace('_',' ').title()} · ${coup['discount_value']}")
                            cc3.write("✅ Active" if coup.get("is_active") else "❌ Inactive")
                            cc4.write(f"{coup.get('valid_from','?')} → {coup.get('valid_until','?')}")
                            with cc5:
                                if st.button("Deactivate" if coup.get("is_active") else "Activate",
                                             key=f"coup_toggle_{coup['coupon_id']}"):
                                    api_patch(f"/grocery/pos/coupons/{coup['coupon_id']}",
                                              {"is_active": not coup.get("is_active")})
                                    st.rerun(scope="fragment")
                                if st.button("🗑", key=f"coup_del_{coup['coupon_id']}"):
                                    api_delete(f"/grocery/pos/coupons/{coup['coupon_id']}")
                                    st.rerun(scope="fragment")
                else:
                    st.info("No coupons found.")

                st.divider()
                with st.expander("➕ Create Coupon"):
                    departments = api_get("/grocery/pos/departments") or []
                    dept_options = {d["name"]: d["department_id"] for d in departments}
                    c1, c2, c3 = st.columns(3)
                    new_code = c1.text_input("Code", key="nc_code")
                    new_type = c2.selectbox("Type", ["percent_off", "dollar_off", "bogo", "free_item"], key="nc_type")
                    new_val = c3.number_input("Discount value", min_value=0.01, value=5.0, key="nc_val")
                    new_desc = st.text_input("Description", key="nc_desc")
                    c4, c5, c6 = st.columns(3)
                    new_dept = c4.selectbox("Department (optional)", ["— None —"] + list(dept_options.keys()), key="nc_dept")
                    new_from = c5.date_input("Valid from", value=date.today(), key="nc_from")
                    new_until = c6.date_input("Valid until", value=date.today() + timedelta(days=30), key="nc_until")
                    new_min = st.number_input("Min purchase ($, 0 = none)", min_value=0.0, value=0.0, key="nc_min")
                    new_max_uses = st.number_input("Max uses (0 = unlimited)", min_value=0, value=0, step=1, key="nc_maxu")
                    if st.button("Create Coupon", type="primary", key="nc_submit"):
                        if not new_code or not new_desc:
                            st.error("Code and description are required.")
                        else:
                            payload = {
                                "code": new_code, "description": new_desc,
                                "coupon_type": new_type, "discount_value": new_val,
                                "valid_from": str(new_from), "valid_until": str(new_until),
                                "min_purchase": new_min if new_min > 0 else None,
                                "max_uses": int(new_max_uses) if new_max_uses > 0 else None,
                                "department_id": dept_options.get(new_dept) if new_dept != "— None —" else None,
                                "is_active": True,
                            }
                            result = api_post("/grocery/pos/coupons", payload)
                            if result:
                                st.success(f"Coupon '{new_code}' created.")
                                st.rerun(scope="fragment")

            # ── Weekly Ads ────────────────────────────────────────────────────────
            with promo_tab_ads:
                st.markdown("#### Weekly Ads")
                ads_resp = api_get("/grocery/pricing/weekly-ads", {"limit": 100})
                ads = (ads_resp.get("data") if isinstance(ads_resp, dict) else ads_resp) or []
                _products_resp = api_get("/grocery/pos/products", {"limit": 2000}) or {}
                products_list = (_products_resp.get("data") if isinstance(_products_resp, dict) else _products_resp) or []
                prod_options = {p["name"]: p["product_id"] for p in products_list}

                if ads:
                    for ad in ads:
                        ad_items_resp = api_get("/grocery/pricing/ad-items", {"ad_id": ad["ad_id"], "limit": 200})
                        ad_items = (ad_items_resp.get("data") if isinstance(ad_items_resp, dict) else ad_items_resp) or []
                        with st.expander(f"📰 {ad['ad_name']} ({ad['start_date']} → {ad['end_date']}) — {len(ad_items)} items"):
                            ac1, ac2 = st.columns([5, 1])
                            with ac2:
                                if st.button("Delete Ad", key=f"ad_del_{ad['ad_id']}"):
                                    api_delete(f"/grocery/pricing/weekly-ads/{ad['ad_id']}")
                                    st.rerun(scope="fragment")

                            if ad_items:
                                for item in ad_items:
                                    ic1, ic2, ic3, ic4 = st.columns([3, 2, 2, 1])
                                    ic1.write(item.get("product_id", "")[:8] + "…")
                                    ic2.write(f"${item['promoted_price']}" if item.get("promoted_price") else "—")
                                    ic3.write(f"{item['discount_pct']}% off" if item.get("discount_pct") else "—")
                                    if ic4.button("✕", key=f"aditem_del_{item['ad_item_id']}"):
                                        api_delete(f"/grocery/pricing/ad-items/{item['ad_item_id']}")
                                        st.rerun(scope="fragment")

                            st.markdown("**Add item to this ad:**")
                            ai1, ai2, ai3, ai4 = st.columns([3, 2, 2, 1])
                            sel_prod = ai1.selectbox("Product", ["— select —"] + list(prod_options.keys()), key=f"ai_prod_{ad['ad_id']}")
                            ai_price = ai2.number_input("Promoted price", min_value=0.0, value=0.0, key=f"ai_price_{ad['ad_id']}")
                            ai_pct = ai3.number_input("Discount %", min_value=0.0, max_value=100.0, value=0.0, key=f"ai_pct_{ad['ad_id']}")
                            if ai4.button("Add", key=f"ai_add_{ad['ad_id']}"):
                                if sel_prod == "— select —":
                                    st.error("Select a product.")
                                else:
                                    api_post("/grocery/pricing/ad-items", {
                                        "ad_id": ad["ad_id"],
                                        "product_id": prod_options[sel_prod],
                                        "promoted_price": ai_price if ai_price > 0 else None,
                                        "discount_pct": ai_pct if ai_pct > 0 else None,
                                    })
                                    st.rerun(scope="fragment")
                else:
                    st.info("No weekly ads found.")

                st.divider()
                with st.expander("➕ Create Weekly Ad"):
                    na1, na2, na3 = st.columns(3)
                    new_ad_name = na1.text_input("Ad name", key="na_name")
                    new_ad_start = na2.date_input("Start date", value=date.today(), key="na_start")
                    new_ad_end = na3.date_input("End date", value=date.today() + timedelta(days=6), key="na_end")
                    if st.button("Create Ad", type="primary", key="na_submit"):
                        if not new_ad_name:
                            st.error("Ad name is required.")
                        elif new_ad_end < new_ad_start:
                            st.error("End date must be after start date.")
                        else:
                            result = api_post("/grocery/pricing/weekly-ads", {
                                "ad_name": new_ad_name,
                                "start_date": str(new_ad_start),
                                "end_date": str(new_ad_end),
                            })
                            if result:
                                st.success(f"Ad '{new_ad_name}' created.")
                                st.rerun(scope="fragment")

    industry = ctx.industry
    pfx = ctx.pfx
    SCHEMA_TABLES = ctx.schema_tables
    TABLE_DOCS = ctx.table_docs

    _promotions()
