"""
Keyword opportunity helpers for Web/API/read-model consumers.
"""
from __future__ import annotations

import json
from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_keyword(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _parse_metadata(raw_value: Any) -> dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if raw_value in (None, "", "null"):
        return {}
    try:
        parsed = json.loads(raw_value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def keyword_quality_flags(keyword: str) -> list[str]:
    normalized = _normalize_keyword(keyword)
    if not normalized:
        return ["blank_keyword"]

    tokens = normalized.split()
    flags: list[str] = []
    if len(tokens) > 8:
        flags.append("too_many_tokens")
    if any(len(token) == 1 for token in tokens):
        flags.append("fragment_token")
    if len(set(tokens)) != len(tokens):
        flags.append("duplicate_tokens")
    if sum(ch.isdigit() for ch in normalized) >= max(4, len(normalized) // 2):
        flags.append("digit_heavy")
    return flags


def _score_supply_gap(row: dict[str, Any]) -> float:
    ratio = _safe_float(row.get("supply_gap_ratio"))
    amazon_total = _safe_float(row.get("amazon_total"))
    noon_total = _safe_float(row.get("noon_total"))
    if amazon_total <= 0:
        return 0.0
    score = max(0.0, min(45.0, (ratio - 1.0) * 8.0))
    if noon_total <= 25 and amazon_total >= 100:
        score += 10.0
    return min(score, 55.0)


def _score_momentum(row: dict[str, Any]) -> float:
    delta = _safe_float(row.get("score_delta"))
    if delta <= 0:
        return 0.0
    return min(20.0, delta * 6.0)


def _score_margin(row: dict[str, Any]) -> float:
    margin = _safe_float(row.get("margin_war_pct"))
    return max(0.0, min(20.0, margin / 2.0))


def _score_competition(row: dict[str, Any]) -> float:
    density = _safe_float(row.get("competition_density"))
    if density <= 0:
        return 0.0
    if density <= 1.0:
        return 16.0
    if density <= 2.0:
        return 12.0
    if density <= 4.0:
        return 8.0
    if density <= 8.0:
        return 4.0
    return 0.0


def _score_watchlist(row: dict[str, Any]) -> float:
    amazon_total = _safe_float(row.get("amazon_total"))
    noon_total = _safe_float(row.get("noon_total"))
    latest_total_score = _safe_float(row.get("latest_total_score"))
    matched_product_count = _safe_int(row.get("matched_product_count"))
    competition_density = _safe_float(row.get("competition_density"))

    score = 0.0
    if amazon_total >= 50:
        score += min(8.0, amazon_total / 80.0)
    if noon_total <= 20:
        score += 5.0
    if matched_product_count <= 12:
        score += 4.0
    if 0 < competition_density <= 2.5:
        score += 3.0
    if latest_total_score >= 5.5:
        score += 3.0
    return min(score, 18.0)


def _score_quality_penalty(row: dict[str, Any]) -> float:
    penalty = float(len(keyword_quality_flags(str(row.get("keyword") or "")))) * 4.0
    matched_product_count = _safe_int(row.get("matched_product_count"))
    if matched_product_count <= 0:
        penalty += 3.0
    return penalty


def _build_reason_bundle(
    *,
    keyword: str,
    supply_gap_score: float,
    momentum_score: float,
    margin_score: float,
    competition_score: float,
    watchlist_score: float,
    penalty: float,
    quality_flags: list[str],
    latest_total_score: float,
    score_delta: float,
    competition_density: float,
    margin_war_pct: float,
    amazon_total: int,
    noon_total: int,
    matched_product_count: int,
    expansion_depth: int,
) -> tuple[list[str], list[str], str]:
    reason_codes: list[str] = []
    reason_summary: list[str] = []

    if supply_gap_score >= 15:
        reason_codes.append("amazon_noon_gap")
        if noon_total > 0:
            reason_summary.append(f"Amazon {amazon_total} vs Noon {noon_total} ({round(max(amazon_total / max(noon_total, 1), 0), 1)}x gap)")
        else:
            reason_summary.append(f"Amazon has {amazon_total} results while Noon supply is near zero")
    if momentum_score >= 8:
        reason_codes.append("score_rising")
        reason_summary.append(f"score improved by {round(score_delta, 2)} vs previous snapshot")
    if margin_score >= 10:
        reason_codes.append("usable_margin")
        reason_summary.append(f"war-time margin still holds at {round(margin_war_pct, 2)}%")
    if competition_score >= 8:
        reason_codes.append("manageable_competition")
        reason_summary.append(f"competition density remains manageable at {round(competition_density, 2)}")
    if watchlist_score >= 10 and supply_gap_score < 15:
        reason_codes.append("early_watchlist")
        reason_summary.append(f"early demand signal exists but Noon only shows {noon_total} results")
    if amazon_total >= 100 and matched_product_count <= 12:
        reason_codes.append("thin_match_set")
        reason_summary.append(f"only {matched_product_count} matched products for {amazon_total} Amazon results")
    if expansion_depth >= 2 and latest_total_score >= 6:
        reason_codes.append("deep_long_tail")
        reason_summary.append(f"depth {expansion_depth} long-tail keyword is already producing usable signal")
    if score_delta < 0:
        reason_codes.append("score_softening")
        reason_summary.append(f"score softened by {round(abs(score_delta), 2)} vs previous snapshot")
    if penalty >= 8:
        reason_codes.append("quality_risk")
    if quality_flags:
        reason_summary.append(f"quality flags: {', '.join(quality_flags[:3])}")

    action_hint = "Keep in tracked pool and wait for stronger demand proof"
    if "amazon_noon_gap" in reason_codes and margin_score >= 8:
        action_hint = "Prioritize Noon validation, pricing review, and seller overlap checks"
    elif "score_rising" in reason_codes:
        action_hint = "Keep this keyword on the next crawl cycle and monitor score acceleration"
    elif "usable_margin" in reason_codes and "manageable_competition" in reason_codes:
        action_hint = "Review pricing and ad pressure before scaling"
    elif noon_total <= 10 and amazon_total >= 80:
        action_hint = "Validate whether Noon supply is structurally thin or just temporarily sparse"

    if not reason_summary:
        reason_summary.append(f"watch keyword '{keyword}' for clearer demand confirmation")

    return reason_codes[:5], reason_summary[:4], action_hint


def _classify_priority_band(opportunity_score: float, risk_level: str) -> str:
    normalized_risk = (risk_level or "").strip().lower()
    if opportunity_score >= 80 and normalized_risk != "high":
        return "critical"
    if opportunity_score >= 60 and normalized_risk != "high":
        return "high"
    if opportunity_score >= 40:
        return "medium"
    return "watch"


def _classify_evidence_strength(*, amazon_total: int, noon_total: int, matched_product_count: int, expansion_depth: int) -> str:
    if amazon_total >= 300 and matched_product_count >= 20 and expansion_depth <= 1:
        return "strong"
    if amazon_total >= 100 and matched_product_count >= 8:
        return "moderate"
    if noon_total <= 10 and amazon_total >= 50:
        return "emerging"
    return "thin"


def _priority_rank(value: Any) -> int:
    mapping = {
        "critical": 4,
        "high": 3,
        "medium": 2,
        "watch": 1,
    }
    return mapping.get(str(value or "").strip().lower(), 0)


def _evidence_rank(value: Any) -> int:
    mapping = {
        "strong": 4,
        "moderate": 3,
        "emerging": 2,
        "thin": 1,
    }
    return mapping.get(str(value or "").strip().lower(), 0)


def classify_opportunity(row: dict[str, Any]) -> dict[str, Any]:
    keyword = _normalize_keyword(row.get("keyword"))
    display_keyword = str(row.get("display_keyword") or keyword)
    metadata = _parse_metadata(row.get("metadata_json"))
    score_delta = round(
        _safe_float(row.get("latest_total_score")) - _safe_float(row.get("previous_total_score")),
        2,
    )
    working_row = dict(row)
    working_row["score_delta"] = score_delta

    supply_gap_score = _score_supply_gap(working_row)
    momentum_score = _score_momentum(working_row)
    margin_score = _score_margin(working_row)
    competition_score = _score_competition(working_row)
    watchlist_score = _score_watchlist(working_row)
    penalty = _score_quality_penalty(working_row)
    latest_total_score = _safe_float(row.get("latest_total_score"))
    amazon_total = _safe_int(row.get("amazon_total"), 0)
    noon_total = _safe_int(row.get("noon_total"), 0)
    matched_product_count = _safe_int(row.get("matched_product_count"), 0)
    expansion_depth = _safe_int(metadata.get("expansion_depth"), 0)
    quality_flags = keyword_quality_flags(keyword)

    opportunity_score = round(
        latest_total_score * 5.0
        + supply_gap_score
        + momentum_score
        + margin_score
        + competition_score
        + watchlist_score
        - penalty,
        2,
    )

    opportunity_type = "watchlist"
    if supply_gap_score >= 20 and margin_score >= 8 and amazon_total >= 80:
        opportunity_type = "supply_gap"
    elif momentum_score >= 8 and latest_total_score >= 6:
        opportunity_type = "momentum"
    elif margin_score >= 10 and competition_score >= 8:
        opportunity_type = "profit_pocket"
    elif watchlist_score >= 10:
        opportunity_type = "watchlist"

    risk_level = "medium"
    if penalty >= 8 or _safe_float(row.get("margin_war_pct")) < 0:
        risk_level = "high"
    elif (
        supply_gap_score >= 20
        and competition_score >= 8
        and penalty == 0
        and _safe_float(row.get("margin_war_pct")) >= 5
    ):
        risk_level = "low"

    reason_codes, reasons, action_hint = _build_reason_bundle(
        keyword=keyword,
        supply_gap_score=supply_gap_score,
        momentum_score=momentum_score,
        margin_score=margin_score,
        competition_score=competition_score,
        watchlist_score=watchlist_score,
        penalty=penalty,
        quality_flags=quality_flags,
        latest_total_score=latest_total_score,
        score_delta=score_delta,
        competition_density=_safe_float(row.get("competition_density")),
        margin_war_pct=_safe_float(row.get("margin_war_pct")),
        amazon_total=amazon_total,
        noon_total=noon_total,
        matched_product_count=matched_product_count,
        expansion_depth=expansion_depth,
    )

    return {
        "keyword": keyword,
        "display_keyword": display_keyword,
        "grade": row.get("grade"),
        "rank": _safe_int(row.get("rank"), 0),
        "latest_total_score": round(latest_total_score, 2),
        "previous_total_score": round(_safe_float(row.get("previous_total_score")), 2),
        "score_delta": score_delta,
        "demand_index": round(_safe_float(row.get("demand_index")), 2),
        "competition_density": round(_safe_float(row.get("competition_density")), 2),
        "supply_gap_ratio": round(_safe_float(row.get("supply_gap_ratio")), 2),
        "margin_war_pct": round(_safe_float(row.get("margin_war_pct")), 2),
        "margin_peace_pct": round(_safe_float(row.get("margin_peace_pct")), 2),
        "noon_total": noon_total,
        "amazon_total": amazon_total,
        "matched_product_count": matched_product_count,
        "tracking_mode": row.get("tracking_mode") or "",
        "source_type": row.get("source_type") or "",
        "source_platform": row.get("source_platform") or "",
        "root_seed_keyword": metadata.get("root_seed_keyword") or keyword,
        "seed_keyword": metadata.get("seed_keyword") or metadata.get("root_seed_keyword") or keyword,
        "expansion_depth": expansion_depth,
        "quality_flags": quality_flags,
        "opportunity_type": opportunity_type,
        "opportunity_score": opportunity_score,
        "risk_level": risk_level,
        "priority_band": _classify_priority_band(opportunity_score, risk_level),
        "evidence_strength": _classify_evidence_strength(
            amazon_total=amazon_total,
            noon_total=noon_total,
            matched_product_count=matched_product_count,
            expansion_depth=expansion_depth,
        ),
        "score_components": {
            "supply_gap": round(supply_gap_score, 2),
            "momentum": round(momentum_score, 2),
            "margin": round(margin_score, 2),
            "competition": round(competition_score, 2),
            "watchlist": round(watchlist_score, 2),
            "quality_penalty": round(penalty, 2),
        },
        "reason_codes": reason_codes,
        "reason_summary": reasons[:3],
        "action_hint": action_hint,
        "decision_summary": f"{reasons[0] if reasons else keyword}. {action_hint}",
    }


def build_opportunity_items(rows: list[dict[str, Any]], *, limit: int = 50) -> list[dict[str, Any]]:
    items = [classify_opportunity(dict(row)) for row in rows]
    items.sort(
        key=lambda item: (
            -_priority_rank(item.get("priority_band")),
            -_safe_float(item.get("opportunity_score")),
            -_evidence_rank(item.get("evidence_strength")),
            -_safe_float(item.get("score_delta")),
            -_safe_float(item.get("latest_total_score")),
            item.get("keyword") or "",
        )
    )
    return items[: max(limit, 0)]


def build_opportunity_summary(
    items: list[dict[str, Any]],
    *,
    limit: int,
    opportunity_type: str = "",
    root_keyword: str = "",
    risk_level: str = "",
    priority_band: str = "",
    evidence_strength: str = "",
) -> dict[str, Any]:
    type_counts: dict[str, int] = {}
    risk_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {}
    root_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    average_score = 0.0
    average_supply_gap = 0.0

    for item in items:
        item_type = str(item.get("opportunity_type") or "").strip().lower()
        item_risk = str(item.get("risk_level") or "").strip().lower()
        item_priority = str(item.get("priority_band") or "").strip().lower()
        item_root = str(item.get("root_seed_keyword") or "").strip().lower()
        type_counts[item_type] = type_counts.get(item_type, 0) + 1
        risk_counts[item_risk] = risk_counts.get(item_risk, 0) + 1
        priority_counts[item_priority] = priority_counts.get(item_priority, 0) + 1
        if item_root:
            root_counts[item_root] = root_counts.get(item_root, 0) + 1
        for code in item.get("reason_codes") or []:
            code_text = str(code).strip().lower()
            if code_text:
                reason_counts[code_text] = reason_counts.get(code_text, 0) + 1
        average_score += _safe_float(item.get("opportunity_score"))
        average_supply_gap += _safe_float(item.get("supply_gap_ratio"))

    item_count = len(items[: max(limit, 0)])
    available_count = len(items)
    divisor = float(available_count or 1)

    top_root_keywords = [
        {"keyword": keyword, "count": count}
        for keyword, count in sorted(root_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:5]
    ]
    top_reason_codes = [
        {"code": code, "count": count}
        for code, count in sorted(reason_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:6]
    ]

    return {
        "item_count": item_count,
        "available_count": available_count,
        "opportunity_type": (opportunity_type or "").strip().lower(),
        "root_keyword": (root_keyword or "").strip().lower(),
        "risk_level": (risk_level or "").strip().lower(),
        "priority_band": (priority_band or "").strip().lower(),
        "evidence_strength": (evidence_strength or "").strip().lower(),
        "type_counts": type_counts,
        "risk_counts": risk_counts,
        "priority_counts": priority_counts,
        "top_root_keywords": top_root_keywords,
        "top_reason_codes": top_reason_codes,
        "low_risk_count": risk_counts.get("low", 0),
        "watchlist_count": type_counts.get("watchlist", 0),
        "avg_opportunity_score": round(average_score / divisor, 2),
        "avg_supply_gap_ratio": round(average_supply_gap / divisor, 2),
    }


def build_quality_issue_summary(items: list[dict[str, Any]], *, limit: int) -> dict[str, Any]:
    flag_counts: dict[str, int] = {}
    root_counts: dict[str, int] = {}

    for item in items:
        root_keyword = str(item.get("root_seed_keyword") or "").strip().lower()
        if root_keyword:
            root_counts[root_keyword] = root_counts.get(root_keyword, 0) + 1
        for flag in item.get("quality_flags") or []:
            flag_text = str(flag).strip().lower()
            if flag_text:
                flag_counts[flag_text] = flag_counts.get(flag_text, 0) + 1

    return {
        "item_count": len(items[: max(limit, 0)]),
        "available_count": len(items),
        "flag_counts": flag_counts,
        "top_root_keywords": [
            {"keyword": keyword, "count": count}
            for keyword, count in sorted(root_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:5]
        ],
        "high_risk_count": sum(1 for item in items if str(item.get("risk_level") or "").lower() == "high"),
    }


def build_keyword_graph_payload(
    root_keyword: str,
    edge_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_root = _normalize_keyword(root_keyword)
    metrics_by_keyword = {
        _normalize_keyword(row.get("keyword")): dict(row)
        for row in metric_rows
        if _normalize_keyword(row.get("keyword"))
    }

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []

    def ensure_node(keyword: str, *, depth: int = 0):
        normalized = _normalize_keyword(keyword)
        if not normalized:
            return
        if normalized in nodes:
            nodes[normalized]["depth"] = min(nodes[normalized]["depth"], depth)
            return
        metric = metrics_by_keyword.get(normalized, {})
        opportunity = classify_opportunity(
            {
                "keyword": normalized,
                "display_keyword": metric.get("display_keyword") or normalized,
                "grade": metric.get("grade"),
                "rank": metric.get("rank"),
                "latest_total_score": metric.get("total_score"),
                "previous_total_score": metric.get("previous_total_score"),
                "demand_index": metric.get("demand_index"),
                "competition_density": metric.get("competition_density"),
                "supply_gap_ratio": metric.get("supply_gap_ratio"),
                "margin_war_pct": metric.get("margin_war_pct"),
                "margin_peace_pct": metric.get("margin_peace_pct"),
                "noon_total": metric.get("noon_total"),
                "amazon_total": metric.get("amazon_total"),
                "matched_product_count": metric.get("matched_product_count"),
                "tracking_mode": metric.get("tracking_mode"),
                "source_type": metric.get("source_type"),
                "source_platform": metric.get("source_platform"),
                "metadata_json": metric.get("metadata_json"),
            }
        )
        nodes[normalized] = {
            "keyword": normalized,
            "display_keyword": opportunity["display_keyword"],
            "depth": depth,
            "is_root": normalized == normalized_root,
            "grade": opportunity["grade"],
            "rank": opportunity["rank"],
            "opportunity_type": opportunity["opportunity_type"],
            "opportunity_score": opportunity["opportunity_score"],
            "priority_band": opportunity["priority_band"],
            "evidence_strength": opportunity["evidence_strength"],
            "latest_total_score": opportunity["latest_total_score"],
            "decision_summary": opportunity["decision_summary"],
            "quality_flags": opportunity["quality_flags"],
        }

    ensure_node(normalized_root, depth=0)

    for row in edge_rows:
        parent = _normalize_keyword(row.get("parent_keyword"))
        child = _normalize_keyword(row.get("child_keyword"))
        if not parent or not child:
            continue
        depth = _safe_int(row.get("depth"), 1)
        ensure_node(parent, depth=max(depth - 1, 0))
        ensure_node(child, depth=depth)
        edges.append(
            {
                "parent_keyword": parent,
                "child_keyword": child,
                "depth": depth,
                "source_platform": row.get("source_platform") or "",
                "source_type": row.get("source_type") or "",
                "discovered_at": row.get("discovered_at") or "",
            }
        )

    edges.sort(key=lambda item: (item["depth"], item["parent_keyword"], item["child_keyword"]))
    ordered_nodes = sorted(nodes.values(), key=lambda item: (item["depth"], item["keyword"]))
    return {
        "root_keyword": normalized_root,
        "node_count": len(ordered_nodes),
        "edge_count": len(edges),
        "nodes": ordered_nodes,
        "edges": edges,
    }
