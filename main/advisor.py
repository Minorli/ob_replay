import re
from typing import Dict, List, Optional, Sequence, Set


def advise(
    sql: str,
    plan: Optional[str] = None,
    known_indexes: Optional[Dict[str, Sequence[Sequence[str]]]] = None,
) -> List[str]:
    """
    Generate lightweight rule-based tuning advice.
    known_indexes: {table: [ [col1, col2], [col3] ]}
    """
    suggestions: List[str] = []
    lowered = sql.lower()

    if "select *" in lowered:
        suggestions.append("避免 SELECT *，只返回需要的列以降低网络与解析开销。")

    where_cols = _extract_where_columns(lowered)
    tables = _extract_tables(lowered)

    if where_cols and tables:
        missing = _find_missing_indexes(tables, where_cols, known_indexes or {})
        if missing:
            for table, cols in missing.items():
                col_list = ", ".join(sorted(cols))
                suggestions.append("表 %s 缺少索引，可考虑在 (%s) 上建索引。" % (table, col_list))

    if re.search(r"like\s+'%[^']*%'", lowered):
        suggestions.append("LIKE '%%pattern%%' 会放弃前缀索引，考虑倒排/前缀匹配或 fulltext。")

    if re.search(r"\b(or|in\s*\()", lowered) and len(where_cols) > 3:
        suggestions.append("WHERE 条件包含大量 OR/IN，考虑拆分或使用分区裁剪以避免全表扫描。")

    if re.search(r"(substr|to_char|upper|lower)\s*\(", lowered):
        suggestions.append("避免在索引列上使用函数，可改写为生成列或函数索引。")

    if plan and "table scan" in plan.lower():
        suggestions.append("执行计划显示 TABLE SCAN，确认是否应增加索引或分区裁剪。")

    if not suggestions:
        suggestions.append("未发现明显问题，可结合 EXPLAIN 结果继续确认。")
    return suggestions


def _extract_where_columns(sql: str) -> Set[str]:
    matches = re.findall(r"where\s+(.+)", sql, flags=re.IGNORECASE | re.DOTALL)
    if not matches:
        return set()
    clause = matches[0]
    columns = re.findall(r"([a-z0-9_\.]+)\s*(=|>|<|in|\blike\b)", clause)
    return {c[0].split(".")[-1] for c in columns}


def _extract_tables(sql: str) -> Set[str]:
    tables = re.findall(r"from\s+([a-z0-9_\.]+)", sql, flags=re.IGNORECASE)
    return {t.split(".")[-1] for t in tables}


def _find_missing_indexes(
    tables: Set[str],
    columns: Set[str],
    known_indexes: Dict[str, Sequence[Sequence[str]]],
) -> Dict[str, Set[str]]:
    missing: Dict[str, Set[str]] = {}
    for table in tables:
        indexed_cols = {col for idx in known_indexes.get(table, []) for col in idx}
        for col in columns:
            if col not in indexed_cols:
                missing.setdefault(table, set()).add(col)
    return missing
