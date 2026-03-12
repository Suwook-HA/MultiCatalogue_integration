"""
데이터 포털 브로커링 시스템 - Streamlit 프론트엔드

브로커를 직접 import하여 사용 (HTTP 서버 불필요).
"""

from __future__ import annotations

import asyncio
import sys
import os

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st

st.set_page_config(
    page_title="통합 데이터 포털 검색",
    page_icon="🔍",
    layout="wide",
)

# ──────────────────────────────────────────────
# 브로커 초기화 (싱글턴, 앱 세션 당 1회)
# ──────────────────────────────────────────────

@st.cache_resource
def get_broker():
    from app.broker.search_broker import SearchBroker
    return SearchBroker()


@st.cache_resource
def get_portals():
    broker = get_broker()
    return broker.get_portals()


def run_async(coro):
    """Streamlit의 동기 컨텍스트에서 비동기 함수를 실행한다."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ──────────────────────────────────────────────
# 사이드바
# ──────────────────────────────────────────────

with st.sidebar:
    st.title("🔍 통합 데이터 검색")
    st.caption("여러 데이터 포털을 한 번에 검색합니다.")

    portals = get_portals()
    portal_options = {p.name: p.id for p in portals if p.enabled}

    st.subheader("포털 선택")
    selected_names = st.multiselect(
        "검색 대상 포털",
        options=list(portal_options.keys()),
        default=list(portal_options.keys()),
        label_visibility="collapsed",
    )
    selected_portal_ids = [portal_options[n] for n in selected_names]

    st.divider()
    st.subheader("필터")

    filter_format = st.text_input("형식 (예: CSV, JSON)", placeholder="")
    filter_theme = st.text_input("주제 (부분 일치)", placeholder="")
    filter_publisher = st.text_input("제공 기관 (부분 일치)", placeholder="")
    col1, col2 = st.columns(2)
    with col1:
        modified_after = st.text_input("수정일 이후", placeholder="2023-01-01")
    with col2:
        modified_before = st.text_input("수정일 이전", placeholder="2024-12-31")

    st.divider()
    dedup = st.toggle("중복 데이터셋 병합", value=True)
    page_size = st.selectbox("페이지당 결과", [5, 10, 20], index=1)

    # 포털 상태
    st.divider()
    if st.button("포털 상태 확인", use_container_width=True):
        with st.spinner("확인 중..."):
            health = run_async(get_broker().health_check())
        for pid, ok in health.items():
            icon = "🟢" if ok else "🔴"
            st.caption(f"{icon} {pid}")


# ──────────────────────────────────────────────
# 세션 상태
# ──────────────────────────────────────────────

if "page" not in st.session_state:
    st.session_state.page = 1
if "last_query" not in st.session_state:
    st.session_state.last_query = ""
if "query_input" not in st.session_state:
    st.session_state.query_input = ""
if "detail_target" not in st.session_state:
    st.session_state.detail_target = None


# ──────────────────────────────────────────────
# 상세 패널
# ──────────────────────────────────────────────

def show_detail(portal_id: str, dataset_id: str, title: str):
    st.session_state.detail_target = (portal_id, dataset_id, title)


if st.session_state.detail_target:
    portal_id, dataset_id, title = st.session_state.detail_target
    with st.expander(f"📄 상세 정보: {title}", expanded=True):
        if st.button("닫기", key="close_detail"):
            st.session_state.detail_target = None
            st.rerun()
        try:
            with st.spinner("불러오는 중..."):
                detail = run_async(get_broker().get_dataset(portal_id, dataset_id))
        except Exception as e:
            st.error(f"조회 실패: {e}")
            detail = None

        if detail:
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"**제목:** {detail.title}")
                st.markdown(f"**포털:** {detail.source_portal_name}")
                st.markdown(f"**식별자:** `{detail.identifier or '-'}`")
                st.markdown(f"**라이선스:** {detail.license or '-'}")
                if detail.publisher:
                    st.markdown(f"**제공 기관:** {detail.publisher.name or '-'}")
            with col_b:
                st.markdown(f"**등록일:** {detail.issued or '-'}")
                st.markdown(f"**수정일:** {detail.modified or '-'}")
                if detail.landing_page:
                    st.markdown(f"**랜딩페이지:** [{detail.landing_page}]({detail.landing_page})")
                if detail.keyword:
                    st.markdown(f"**키워드:** {', '.join(detail.keyword)}")

            if detail.description:
                st.markdown("**설명:**")
                desc = detail.description
                st.info(desc[:800] + ("..." if len(desc) > 800 else ""))

            if detail.distribution:
                st.markdown(f"**배포 ({len(detail.distribution)}건):**")
                rows = [
                    {
                        "형식": d.format or "-",
                        "제목": d.title or "-",
                        "URL": d.access_url or d.download_url or "-",
                    }
                    for d in detail.distribution[:15]
                ]
                st.dataframe(rows, use_container_width=True)

            if detail.also_available_at:
                st.markdown("**다른 포털에서도 제공:**")
                for ref in detail.also_available_at:
                    lp = ref.get("landing_page", "")
                    label = f"{ref.get('portal_name', ref.get('portal_id', ''))} ({ref.get('source_id', '')})"
                    st.markdown(f"  - [{label}]({lp})" if lp else f"  - {label}")

            if detail.extras:
                with st.expander("원본 추가 필드 (extras)"):
                    st.json(detail.extras)

            if detail.mapping_suggestions:
                with st.expander(f"매핑 제안 ({len(detail.mapping_suggestions)}건)"):
                    for s in detail.mapping_suggestions:
                        conf = s.confidence
                        icon = "🟢" if conf >= 0.8 else ("🟡" if conf >= 0.6 else "🔴")
                        st.markdown(
                            f"{icon} `{s.source_field}` → **{s.suggested_dcat}** "
                            f"(confidence: {conf:.2f}) — {s.reason}"
                        )


# ──────────────────────────────────────────────
# 메인 검색 영역
# ──────────────────────────────────────────────

st.header("통합 데이터 포털 검색")

search_col, btn_col = st.columns([5, 1])
with search_col:
    query = st.text_input(
        "검색어",
        placeholder="검색할 키워드를 입력하세요 (예: 교통, transport, climate)",
        label_visibility="collapsed",
        key="query_input",
    )
with btn_col:
    search_btn = st.button("검색", type="primary", use_container_width=True)

if query != st.session_state.last_query:
    st.session_state.page = 1
    st.session_state.last_query = query

if (search_btn or query) and query.strip():
    try:
        with st.spinner(f"'{query}' 검색 중..."):
            result = run_async(
                get_broker().search(
                    query=query.strip(),
                    portal_ids=selected_portal_ids or None,
                    page=st.session_state.page,
                    size=page_size,
                    filter_format=filter_format.strip() or None,
                    filter_theme=filter_theme.strip() or None,
                    filter_publisher=filter_publisher.strip() or None,
                    filter_license=None,
                    modified_after=modified_after.strip() or None,
                    modified_before=modified_before.strip() or None,
                    dedup=dedup,
                )
            )
    except Exception as e:
        st.error(f"검색 오류: {e}")
        result = None

    if result:
        total = result.total
        datasets = result.datasets
        portals_searched = result.portals_searched
        portals_failed = result.portals_failed
        facets = result.facets
        cached = result.cached

        # 상태 바
        status_parts = [f"**{total:,}건** 발견", f"포털 {len(portals_searched)}개 검색"]
        if portals_failed:
            status_parts.append(f"⚠️ {len(portals_failed)}개 실패: {', '.join(portals_failed)}")
        if cached:
            status_parts.append("⚡ 캐시")
        st.caption(" · ".join(status_parts))

        # 패싯 집계
        facet_data = [
            ("형식", facets.formats[:5]),
            ("주제", facets.themes[:5]),
            ("제공 기관", facets.publishers[:5]),
            ("라이선스", facets.licenses[:3]),
        ]
        non_empty = [(label, items) for label, items in facet_data if items]
        if non_empty:
            facet_cols = st.columns(len(non_empty))
            for col, (label, items) in zip(facet_cols, non_empty):
                with col:
                    with st.expander(f"**{label}** ({len(items)})", expanded=False):
                        for item in items:
                            st.caption(f"{item.value} ({item.count})")

        st.divider()

        if not datasets:
            st.info("검색 결과가 없습니다.")
        else:
            for ds in datasets:
                title = ds.title or "(제목 없음)"
                portal_name = ds.source_portal_name or ds.source_portal
                source_id = ds.source_id or ds.identifier or ""
                portal_id = ds.source_portal
                description = ds.description or ""
                formats = sorted({d.format for d in ds.distribution if d.format})
                modified = str(ds.modified or "")[:10]
                also_at = ds.also_available_at

                with st.container(border=True):
                    h_col, btn_col2 = st.columns([6, 1])
                    with h_col:
                        title_display = (
                            f"[{title}]({ds.landing_page})" if ds.landing_page else title
                        )
                        st.markdown(f"#### {title_display}")

                        badge_parts = [f"🏛️ `{portal_name}`"]
                        if formats:
                            badge_parts.append("  ".join(f"`{f}`" for f in formats[:5]))
                        if modified:
                            badge_parts.append(f"📅 {modified}")
                        if also_at:
                            badge_parts.append(f"🔗 {len(also_at)}개 포털 동시 제공")
                        st.caption("  ·  ".join(badge_parts))

                    with btn_col2:
                        if source_id and portal_id:
                            if st.button("상세", key=f"detail_{portal_id}_{source_id}"):
                                show_detail(portal_id, source_id, title)
                                st.rerun()

                    if description:
                        st.markdown(description[:250] + ("..." if len(description) > 250 else ""))

                    tag_parts = [f"🏷️ {t}" for t in ds.theme[:3]] + [f"#{kw}" for kw in ds.keyword[:5]]
                    if tag_parts:
                        st.caption("  ".join(tag_parts))

        # 페이지네이션
        total_pages = max(1, (total + page_size - 1) // page_size)
        if total_pages > 1:
            st.divider()
            pg_cols = st.columns([1, 3, 1])
            with pg_cols[0]:
                if st.button("◀ 이전", disabled=st.session_state.page <= 1):
                    st.session_state.page -= 1
                    st.rerun()
            with pg_cols[1]:
                st.caption(f"페이지 {st.session_state.page} / {total_pages}")
            with pg_cols[2]:
                if st.button("다음 ▶", disabled=st.session_state.page >= total_pages):
                    st.session_state.page += 1
                    st.rerun()

else:
    st.markdown(
        """
        #### 시작하기

        왼쪽 사이드바에서 **검색 대상 포털**을 선택하고 위 검색창에 키워드를 입력하세요.

        지원 포털:
        - 🇰🇷 **공공데이터포털** (data.go.kr) — API 키 필요
        - 📦 **CKAN** 호환 포털 (demo.ckan.org)
        - 🇪🇺 **DCAT/RDF** 포털 (data.europa.eu)

        기능:
        - 복수 포털 **병렬 검색** + 포털 장애 시 부분 성공
        - 결과를 **W3C DCAT v2** 표준으로 정규화
        - 형식·주제·제공기관·날짜 **필터**
        - **패싯 집계** (형식별/주제별 건수)
        - 교차 포털 **중복 탐지 및 병합**
        - 미지 필드 **자동 매핑 제안**
        """
    )
