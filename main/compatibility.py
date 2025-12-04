from typing import Optional

from .models import CompatibilityIssue
from .oceanbase_client import OceanBaseClient


def check_sql(
    ob_client: OceanBaseClient, sql: str, execute: bool = False
) -> CompatibilityIssue:
    """
    Check whether a SQL can be parsed and planned on OceanBase.
    Optionally executes the SQL (use with read-only statements).
    """
    explain_result = ob_client.explain(sql)
    if not explain_result.success:
        return CompatibilityIssue(
            sql=sql,
            is_supported=False,
            stage="explain",
            error_message=explain_result.error_message,
            hint=_hint_from_error(explain_result.error_message),
        )

    if not execute:
        return CompatibilityIssue(
            sql=sql,
            is_supported=True,
            stage="explain",
            plan=explain_result.plan,
        )

    exec_result = ob_client.execute(sql)
    if exec_result.success:
        return CompatibilityIssue(
            sql=sql,
            is_supported=True,
            stage="execute",
            plan=explain_result.plan,
        )
    return CompatibilityIssue(
        sql=sql,
        is_supported=False,
        stage="execute",
        error_message=exec_result.error_message,
        hint=_hint_from_error(exec_result.error_message),
        plan=explain_result.plan,
    )


def _hint_from_error(message: Optional[str]) -> Optional[str]:
    if not message:
        return None
    msg = message.lower()
    if "not supported" in msg or "feature not supported" in msg:
        return "OceanBase Oracle 模式暂不支持该语法，请改写或升级版本后再试。"
    if "syntax error" in msg:
        return "检查 Oracle 专有语法或函数，必要时使用兼容写法。"
    if "permission" in msg or "privilege" in msg:
        return "检查目标库用户权限，与 Oracle 源库保持一致。"
    if "table or view does not exist" in msg:
        return "确认迁移后的表/视图是否存在，或补充同名同结构对象。"
    return None
