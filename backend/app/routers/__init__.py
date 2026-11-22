"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import ddl_batch, resource

# ddl_batch 必须先注册：/api/schema_evo/confirmations 等固定路径
# 不能被资源路由的 /api/{module}/{entry_id} 动态路径抢先匹配。
ROUTERS = [ddl_batch, resource]
