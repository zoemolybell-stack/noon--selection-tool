"""
标准化数据接口定义

所有数据源（爬虫、API、用户输入）的输出都应符合以下 schema。
新增数据源时，只需要实现从原始数据到标准 schema 的转换。
"""

# 关键词级别标准数据
KEYWORD_SCHEMA = {
    "keyword": str,              # 搜索关键词
    "platform": str,             # 数据来源平台
    "snapshot_id": str,          # 所属快照ID
    "scraped_at": str,           # 爬取时间 ISO格式
    "total_results": int,        # 搜索结果总数
    "median_price": float,       # 中位价（当地货币）
    "avg_rating": float,         # 平均评分
    "avg_reviews": int,          # 平均评论数
    "category": str,             # 平台品类
    "extra": dict,               # 平台特有字段（如 express_results, bsr_count）
}

# 商品级别标准数据
PRODUCT_SCHEMA = {
    "keyword": str,              # 来自哪个搜索词
    "platform": str,             # 数据来源平台
    "snapshot_id": str,          # 所属快照ID
    "product_id": str,           # 平台商品ID（如 Noon的SKU、Amazon的ASIN）
    "title": str,                # 商品标题
    "price": float,              # 当前售价（当地货币）
    "currency": str,             # 货币代码（SAR/AED/USD）
    "rating": float,             # 评分
    "review_count": int,         # 评论数
    "seller_name": str,          # 卖家名
    "brand": str,                # 品牌
    "product_url": str,          # 商品链接
    "image_count": int,          # 图片数
    "extra": dict,               # 平台特有字段
}

# 未来：销售数据标准接口（Noon Seller API 接入后使用）
SALES_SCHEMA = {
    "product_id": str,           # 产品ID
    "date": str,                 # 日期
    "units_sold": int,           # 销量
    "revenue": float,            # 收入
    "returns": int,              # 退货数
    "platform_fees": float,      # 平台扣费
    "settlement_amount": float,  # 实际结算金额
}
