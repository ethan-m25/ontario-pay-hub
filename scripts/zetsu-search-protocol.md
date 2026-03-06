# zetsu 搜索协议 — Ontario Pay Hub 每日情报

## 任务目标
每天搜索最新的 Ontario 职位招聘广告，提取含有薪酬区间的 posting，格式化为 JSON 写入 `/tmp/ontario-jobs-raw-YYYY-MM-DD.txt`。

## 搜索质量铁律

### 有效数据标准（必须同时满足）
1. **地域**: Ontario, Canada（不接受 BC / AB / 美国职位）
2. **薪酬格式**: 明确的 CAD min-max 区间（如 "$85,000 - $120,000"）
   - 拒绝: 单一数字（"up to $100k"）
   - 拒绝: 非 CAD 货币
   - 拒绝: 时薪（hourly）除非能换算为合理年薪
3. **日期**: posted >= 2026-01-01
4. **来源**: 公司官网 careers 页面 / Indeed.ca / LinkedIn.com（加拿大版）

### 拒绝标准（遇到立即跳过）
- 职位不在 Ontario
- 无薪酬区间（Pay Transparency Act 要求合规雇主必须披露）
- min >= max（数据错误）
- min < $25,000 或 max > $800,000（异常值）
- 招聘中介发布，无明确雇主

## 搜索查询模板（每次运行使用全部 5 组）

```
Query 1: "Ontario" "salary" "$" "-" "CAD" site:indeed.ca 2026
Query 2: "Toronto" OR "Mississauga" "pay range" "$" site:linkedin.com/jobs 2026
Query 3: "compensation range" "Ontario" "CAD" job posting 2026
Query 4: site:scotiabank.com OR site:rbc.com OR site:shopify.com careers "salary range" Ontario 2026
Query 5: "Ontario Pay Transparency" salary range job posting site:ca.indeed.com
```

## 输出格式（每行一个 JSON 对象）

```jsonl
{"role":"Software Engineer","company":"Shopify","min":115000,"max":162000,"location":"Toronto, ON","source_url":"https://shopify.com/careers/job-123","posted":"2026-03-05"}
{"role":"Data Analyst","company":"TD Bank","min":82000,"max":118000,"location":"Toronto, ON","source_url":"https://td.com/careers/analyst-456","posted":"2026-03-04"}
```

## 数据交叉验证规则

1. **双源验证**: 尽量找到至少 2 个来源确认同一职位的薪酬区间
2. **差异处理**: 若两个来源薪酬差 > 20%，以官网为准，注明差异
3. **可信度打分**:
   - HIGH: 公司官网直接 posting
   - MEDIUM: Indeed/LinkedIn 有明确薪酬显示
   - LOW: 第三方聚合，仅作参考

## 每日运行后写入文件

```bash
TODAY=$(date +%Y-%m-%d)
# 写搜索结果到 (每行一个 JSON object)
OUTPUT_FILE="/tmp/ontario-jobs-raw-$TODAY.txt"
```

然后通知 kisame 运行 update-jobs.sh：
```bash
bash ~/ontario-pay-hub/scripts/update-jobs.sh
```

## 质量检查清单（写文件前自检）
- [ ] 每个 entry 都有 role, company, min, max, source_url, posted
- [ ] min < max，且范围合理（$40k - $500k）
- [ ] location 包含 "ON" 或 "Ontario"
- [ ] source_url 可访问（不是 404）
- [ ] posted date 在 2026-01-01 之后
