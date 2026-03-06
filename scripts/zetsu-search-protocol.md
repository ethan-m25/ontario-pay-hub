# zetsu 搜索协议 — Ontario Pay Hub 每日情报

## 任务目标
每天搜索最新的 Ontario 职位招聘广告，提取含有薪酬区间的 posting，格式化为 JSON 写入 `/tmp/ontario-jobs-raw-YYYY-MM-DD.txt`。

---

## ⚠️ 核心要求：source_url 必须是职位具体页面

**绝对禁止**: 公司招聘主页 / 首页 / 通用搜索页

| ❌ 拒绝（主页级别） | ✅ 接受（职位专页） |
|---|---|
| `https://shopify.com/careers` | `https://www.shopify.com/careers/senior-engineer-toronto_remote-toronto-on_full-time` |
| `https://rbc.com/careers` | `https://jobs.rbc.com/ca/en/job/R-0000123456/Senior-Analyst` |
| `https://td.com/careers` | `https://ca.indeed.com/viewjob?jk=abc1234567890` |
| `https://ca.indeed.com/jobs` | `https://ca.linkedin.com/jobs/view/3890123456` |
| `https://linkedin.com/jobs` | `https://www.scotiabank.com/careers/en/jobs/detail/12345-Software-Engineer` |

**判断标准**: URL 必须包含以下之一：
- 数字 job ID（如 `jk=abc123`、`/job/R-0000123456`、`/12345`）
- 或 URL 路径中有职位名称（如 `/senior-engineer-toronto`）
- Indeed: `ca.indeed.com/viewjob?jk=` ← 这是正确格式
- LinkedIn: `linkedin.com/jobs/view/数字ID` ← 这是正确格式
- 无法找到具体URL的职位 → **丢弃该条，不要填写通用主页**

---

## 搜索质量铁律

### 有效数据标准（必须同时满足）
1. **地域**: Ontario, Canada（不接受 BC / AB / 美国职位）
2. **薪酬格式**: 明确的 CAD min-max 区间（如 "$85,000 - $120,000"）
   - 拒绝: 单一数字（"up to $100k"）
   - 拒绝: 非 CAD 货币
   - 拒绝: 时薪（hourly）除非能换算为合理年薪
3. **日期**: posted >= 2026-01-01
4. **来源**: 公司官网 careers 专属职位页 / Indeed.ca 具体职位页 / LinkedIn 具体职位页

### 拒绝标准（遇到立即跳过）
- 职位不在 Ontario
- 无薪酬区间
- source_url 是通用主页（见上表）
- min >= max（数据错误）
- min < $30,000 或 max > $700,000（异常值）
- 招聘中介发布，无明确雇主

---

## 每日例行搜索查询（每次运行使用全部 3 组）

```
Query 1: Ontario job "salary" "$" "-" "CAD" site:ca.indeed.com 2026
Query 2: "Toronto" OR "Mississauga" "pay range" "$" site:linkedin.com/jobs 2026
Query 3: "compensation range" "Ontario" "CAD" job posting 2026
```

---

## 历史数据补全任务（一次性，2026-01-01 至今）

当收到 `BULK-SEARCH-HISTORICAL` 任务时，依次运行以下全部 24 组查询：

### Tech / Engineering
```
Q01: ca.indeed.com "Software Engineer" Ontario salary "$" 2026 "per year"
Q02: ca.indeed.com "Data Engineer" OR "Data Analyst" Ontario salary range CAD 2026
Q03: ca.indeed.com "DevOps" OR "Cloud Engineer" Ontario salary "$" 2026
Q04: ca.indeed.com "Product Manager" Ontario "pay range" CAD 2026
Q05: ca.indeed.com "UX Designer" OR "UI Designer" Ontario salary range 2026
Q06: linkedin.com jobs "Software Engineer" Toronto Mississauga "pay range" 2026
```

### Finance / Banking
```
Q07: ca.indeed.com "Financial Analyst" Ontario salary range "$" CAD 2026
Q08: ca.indeed.com "Accountant" Ontario "compensation" "$" range 2026
Q09: ca.indeed.com "Risk Analyst" OR "Compliance" Toronto salary range 2026
Q10: ca.indeed.com "Portfolio Manager" OR "Investment" Ontario CAD salary 2026
```

### Healthcare / Life Sciences
```
Q11: ca.indeed.com "Registered Nurse" Ontario "salary" "$" 2026 "per year"
Q12: ca.indeed.com "Pharmacist" OR "Physiotherapist" Ontario salary range CAD 2026
Q13: ca.indeed.com "Medical" OR "Clinical" Ontario pay range salary "$" 2026
```

### Government / Public Sector
```
Q14: ca.indeed.com "Policy Analyst" Ontario government salary range 2026
Q15: ca.indeed.com "Program Coordinator" OR "Program Manager" Ontario salary 2026
Q16: site:ontario.ca/page/careers OR careers.ontario.ca salary range 2026
```

### HR / Compensation / Legal
```
Q17: ca.indeed.com "HR Manager" OR "Compensation Manager" Ontario salary range CAD 2026
Q18: ca.indeed.com "Recruiter" OR "Talent Acquisition" Ontario pay range "$" 2026
Q19: ca.indeed.com "Paralegal" OR "Legal Counsel" Ontario salary range 2026
```

### Marketing / Operations / Other
```
Q20: ca.indeed.com "Marketing Manager" Toronto salary range "$" CAD 2026
Q21: ca.indeed.com "Supply Chain" OR "Logistics" Ontario salary "$" 2026
Q22: ca.indeed.com "Civil Engineer" OR "Mechanical Engineer" Ontario salary range 2026
Q23: "Ontario" "salary range" "$" job posting site:workopolis.com 2026
Q24: "Ontario" "pay range" "$" CAD job 2026 site:glassdoor.ca OR site:jobbank.gc.ca
```

### 历史任务输出要求
- 目标：每个 query 至少 3 条有效 entry（总目标 50+ 条真实数据）
- 每条必须有真实可访问的具体职位 URL
- 覆盖日期范围：2026-01-01 至今（按 posted 日期排序）
- 将全部结果写入 `/tmp/ontario-jobs-raw-$(date +%Y-%m-%d).txt`

---

## 输出格式（每行一个 JSON 对象）

```jsonl
{"role":"Software Engineer","company":"Shopify","min":115000,"max":162000,"location":"Toronto, ON","source_url":"https://www.shopify.com/careers/senior-backend-engineer-core_remote-toronto-on_full-time","posted":"2026-03-05"}
{"role":"Data Analyst","company":"TD Bank","min":82000,"max":118000,"location":"Toronto, ON","source_url":"https://ca.indeed.com/viewjob?jk=abc1234567890def","posted":"2026-03-04"}
```

---

## 质量检查清单（写文件前自检）

- [ ] 每个 entry 都有 role, company, min, max, source_url, posted
- [ ] min < max，且范围合理（$30k - $700k）
- [ ] location 包含 "ON" 或 "Ontario"
- [ ] source_url **不是**通用主页（必须是具体职位页）
- [ ] source_url 包含 job ID 或具体职位名称路径
- [ ] posted date 在 2026-01-01 之后
- [ ] 来源是真实存在的职位（不是杜撰）
