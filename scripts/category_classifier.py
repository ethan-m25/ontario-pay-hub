import re
import unicodedata


CATEGORY_TO_TAG = {
    "Engineering": "eng",
    "Data & Analytics": "data",
    "Finance": "fin",
    "Product & Project": "pm",
    "Sales & Mktg": "sales",
    "People & HR": "hr",
    "Operations": "ops",
    "Legal": "legal",
    "IT & Infra": "it",
    "Leadership": "exec",
    "Other": "other",
}

CATEGORY_PRIORITY = [
    "People & HR",
    "Finance",
    "Legal",
    "IT & Infra",
    "Engineering",
    "Data & Analytics",
    "Product & Project",
    "Sales & Mktg",
    "Operations",
    "Leadership",
    "Other",
]

NORMALIZE_REPLACEMENTS = {
    " sr ": " senior ",
    " jr ": " junior ",
    " mgr ": " manager ",
    " vp ": " vice president ",
    " avp ": " assistant vice president ",
    " dir ": " director ",
    " hrbp ": " hr business partner ",
    " biz dev ": " business development ",
    " qa ": " quality assurance ",
    " ui/ux ": " ui ux ",
    " ui ux ": " design ",
    " fp&a ": " fpanda ",
    " m&a ": " mergers acquisitions ",
}

LEADERSHIP_TITLES = (
    "chief",
    "vice president",
    "assistant vice president",
    "head of",
    "managing director",
    "executive director",
    "senior director",
    "director",
    "general manager",
    "regional manager",
    "country lead",
)

COMPANY_HINTS = {
    "finance_org": (
        "td bank",
        "bmo",
        "rbc",
        "cibc",
        "scotiabank",
        "manulife",
        "sun life",
        "omers",
        "brookfield",
        "wealthsimple",
        "intact",
    ),
    "public_org": (
        "government",
        "commission",
        "city of",
        "public health",
        "ontario health",
        "hydro one",
        "metrolinx",
        "cadth",
    ),
}

CATEGORY_RULES = {
    "People & HR": {
        "strong_phrases": [
            "human resources",
            "hr business partner",
            "talent acquisition",
            "talent management",
            "people business partner",
            "employee relations",
            "labour relations",
            "learning and development",
            "leadership development",
            "total rewards",
            "people operations",
        ],
        "strong_tokens": [
            "recruiter",
            "recruitment",
            "recruiting",
            "compensation",
            "benefits",
            "payroll",
            "hr",
            "talent",
            "people partner",
            "people",
        ],
        "weak_tokens": [
            "learning",
            "development",
            "culture",
            "workforce",
        ],
        "negative_phrases": [],
        "negative_tokens": [],
    },
    "Finance": {
        "strong_phrases": [
            "financial analyst",
            "financial planning",
            "financial reporting",
            "commercial banking",
            "capital markets",
            "private wealth",
            "wealth management",
            "credit risk",
            "investment banking",
            "asset management",
            "portfolio manager",
            "private equity",
            "fund accountant",
            "underwriter",
            "actuarial",
            "treasury",
            "account management",
            "reinsurance",
        ],
        "strong_tokens": [
            "finance",
            "financial",
            "accounting",
            "accountant",
            "banking",
            "credit",
            "tax",
            "treasury",
            "actuary",
            "underwriting",
            "portfolio",
            "investment",
            "lending",
            "controller",
            "bookkeeper",
            "fund",
            "capital",
            "securities",
        ],
        "weak_tokens": [
            "wealth",
            "insurance",
            "pricing",
            "claims",
            "fpanda",
        ],
        "negative_phrases": [
            "business analyst",
            "data analyst",
            "marketing manager",
        ],
        "negative_tokens": [],
    },
    "Legal": {
        "strong_phrases": [
            "legal counsel",
            "general counsel",
            "compliance officer",
            "compliance manager",
            "compliance analyst",
            "risk management",
            "operational risk",
            "market risk",
            "model risk",
            "model validation",
            "anti money laundering",
            "retail insurance compliance",
            "complaints officer",
            "patent engineering",
        ],
        "strong_tokens": [
            "legal",
            "counsel",
            "attorney",
            "compliance",
            "risk",
            "audit",
            "governance",
            "aml",
            "regulatory",
            "controls",
            "policy",
            "investigation",
            "privacy",
            "paralegal",
            "patent",
        ],
        "weak_tokens": [
            "ethics",
            "fraud",
            "oversight",
        ],
        "negative_phrases": [],
        "negative_tokens": [],
    },
    "IT & Infra": {
        "strong_phrases": [
            "cloud architect",
            "solution architect",
            "solutions architect",
            "enterprise architect",
            "site reliability",
            "data architect",
            "database administrator",
            "systems administrator",
            "network administrator",
            "desktop support",
            "it support",
            "information security",
            "cyber threat",
            "threat exposure",
            "cyber security",
            "cybersecurity",
            "enterprise systems",
            "api platform",
            "m365",
            "servicenow",
            "trading platform",
            "purview",
        ],
        "strong_tokens": [
            "cloud",
            "infrastructure",
            "platform",
            "architect",
            "security",
            "network",
            "database",
            "dba",
            "devops",
            "sre",
            "systems",
            "it",
            "infrastructure",
            "api",
        ],
        "weak_tokens": [
            "support",
            "administrator",
            "ops",
            "enterprise",
            "technology",
            "integration",
        ],
        "negative_phrases": [
            "technology delivery",
            "business analyst",
        ],
        "negative_tokens": [],
    },
    "Engineering": {
        "strong_phrases": [
            "software engineer",
            "software engineering",
            "software developer",
            "machine learning engineer",
            "ai engineer",
            "full stack",
            "front end",
            "frontend",
            "back end",
            "backend",
            "mobile engineer",
            "mobile developer",
            "quality assurance automation",
            "quality assurance engineer",
            "application engineer",
            "application engineering",
            "application developer",
            "software development",
            "development manager",
            "director of development",
            "systems developer",
            "lead developer",
            "design systems",
        ],
        "strong_tokens": [
            "developer",
            "engineer",
            "software",
            "coding",
            "ios",
            "android",
            "firmware",
            "embedded",
            "technologist",
        ],
        "weak_tokens": [
            "development",
            "automation",
        ],
        "negative_phrases": [
            "cloud architect",
            "solution architect",
            "solutions architect",
            "business analyst",
            "technology delivery",
            "product support",
            "trading platform",
        ],
        "negative_tokens": [
            "compliance",
        ],
    },
    "Data & Analytics": {
        "strong_phrases": [
            "data scientist",
            "data analyst",
            "data engineer",
            "data science",
            "data governance",
            "data management",
            "business intelligence",
            "analytics engineer",
            "decision science",
            "quantitative analyst",
            "research scientist",
            "insights manager",
            "data architect",
            "bi reporting",
            "reporting developer",
        ],
        "strong_tokens": [
            "analytics",
            "reporting",
            "insights",
            "measurement",
            "research",
            "bi",
            "scientist",
            "data",
            "governance",
            "scientific",
        ],
        "weak_tokens": [
            "report",
            "modeling",
            "researcher",
        ],
        "negative_phrases": [
            "business analyst",
            "financial analyst",
            "risk analyst",
            "compliance analyst",
            "strategy and operations",
        ],
        "negative_tokens": [],
    },
    "Product & Project": {
        "strong_phrases": [
            "product manager",
            "product owner",
            "project manager",
            "program manager",
            "business analyst",
            "delivery manager",
            "release manager",
            "release train engineer",
            "scrum master",
            "agile coach",
            "technical program manager",
            "change manager",
            "transformation manager",
            "technology delivery",
            "service delivery",
            "product support",
            "workflows",
        ],
        "strong_tokens": [
            "product",
            "project",
            "program",
            "delivery",
            "implementation",
            "transformation",
            "change",
            "release",
            "pmo",
        ],
        "weak_tokens": [
            "coordinator",
            "planning",
            "roadmap",
        ],
        "negative_phrases": [
            "product support",
            "customer support",
        ],
        "negative_tokens": [],
    },
    "Sales & Mktg": {
        "strong_phrases": [
            "business development",
            "account executive",
            "account manager",
            "digital marketing",
            "brand manager",
            "public relations",
            "investor relations",
            "sales enablement",
            "campaign strategy",
            "campaign optimization",
            "alternatives distribution",
        ],
        "strong_tokens": [
            "sales",
            "marketing",
            "brand",
            "communications",
            "campaign",
            "growth",
            "partnership",
            "distribution",
            "events",
            "commerce",
            "merchandising",
        ],
        "weak_tokens": [
            "client success",
            "customer success",
            "enablement",
            "demand",
        ],
        "negative_phrases": [],
        "negative_tokens": [],
    },
    "Operations": {
        "strong_phrases": [
            "operations manager",
            "operations coordinator",
            "operations analyst",
            "executive assistant",
            "office manager",
            "office coordinator",
            "service representative",
            "customer service",
            "client service",
            "contact centre",
            "contact center",
            "supply chain",
            "service delivery",
            "maintenance supervisor",
            "claims specialist",
        ],
        "strong_tokens": [
            "operations",
            "coordinator",
            "administrator",
            "procurement",
            "processing",
            "logistics",
            "facilities",
            "scheduling",
            "support",
        ],
        "weak_tokens": [
            "assistant",
            "workflow",
            "continuous improvement",
        ],
        "negative_phrases": [
            "it support",
            "desktop support",
            "product support",
            "business analyst",
        ],
        "negative_tokens": [],
    },
    "Leadership": {
        "strong_phrases": [
            "general manager",
            "regional manager",
            "country lead",
        ],
        "strong_tokens": [
            "chief",
            "vice president",
            "director",
            "head",
        ],
        "weak_tokens": [],
        "negative_phrases": [],
        "negative_tokens": [],
    },
    "Other": {
        "strong_phrases": [
            "career event",
            "recruitment event",
            "job fair",
            "information session",
        ],
        "strong_tokens": [],
        "weak_tokens": [],
        "negative_phrases": [],
        "negative_tokens": [],
    },
}


def normalize_title(text):
    text = unicodedata.normalize("NFKD", str(text or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[&/,+()\-]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = " " + re.sub(r"\s+", " ", text).strip() + " "
    for src, dst in NORMALIZE_REPLACEMENTS.items():
        text = text.replace(src, dst)
    return " " + re.sub(r"\s+", " ", text).strip() + " "


def normalize_category(value):
    value = str(value or "").strip()
    aliases = {
        "Engineering": "Engineering",
        "Data & Analytics": "Data & Analytics",
        "Finance": "Finance",
        "Product & Project": "Product & Project",
        "Sales & Mktg": "Sales & Mktg",
        "People & HR": "People & HR",
        "Operations": "Operations",
        "Legal": "Legal",
        "IT & Infra": "IT & Infra",
        "Leadership": "Leadership",
        "Other": "Other",
        "Sales & Marketing": "Sales & Mktg",
        "People and HR": "People & HR",
        "IT & Infrastructure": "IT & Infra",
    }
    return aliases.get(value, "Other")


def _contains(text, term):
    term = f" {term.strip()} "
    return term in text


def _score_bucket(text, bucket_terms, weight, matched, label):
    score = 0
    seen = set()
    for term in bucket_terms:
        if term in seen:
            continue
        if _contains(text, term):
            seen.add(term)
            score += weight
            matched.append(f"{label}:{term.strip()}")
    return score


def _apply_special_cases(job, text, scores, matched):
    company = str(job.get("company", "")).lower()

    if _contains(text, " business analyst "):
        scores["Product & Project"] += 5
        matched["Product & Project"].append("rule:business analyst default")
        scores["Data & Analytics"] -= 4
        matched["Data & Analytics"].append("neg:business analyst")
        if any(_contains(text, phrase) for phrase in (" data business analyst ", " analytics business analyst ")):
            scores["Data & Analytics"] += 7
            matched["Data & Analytics"].append("rule:data business analyst")
        if _contains(text, " finance business analyst "):
            scores["Finance"] += 7
            matched["Finance"].append("rule:finance business analyst")
        if any(_contains(text, phrase) for phrase in (" investment ", " trading ", " treasury ", " portfolio ", " capital markets ")):
            scores["Finance"] += 5
            matched["Finance"].append("rule:business analyst + finance domain")
        if any(_contains(text, phrase) for phrase in (" platform ", " cloud ", " security ", " integration ", " systems ", " technology ")):
            scores["IT & Infra"] += 5
            matched["IT & Infra"].append("rule:business analyst + technology/platform")
        if _contains(text, " operations "):
            scores["Operations"] += 2
            matched["Operations"].append("rule:business analyst + operations")

    if any(_contains(text, term) for term in (" risk ", " compliance ", " audit ", " governance ", " aml ", " controls ")):
        scores["Legal"] += 4
        matched["Legal"].append("rule:risk/compliance/audit/governance")
        scores["Leadership"] -= 2
        matched["Leadership"].append("neg:function clearer than leadership")
        if any(_contains(text, phrase) for phrase in (" data scientist ", " data analyst ", " data architect ", " data engineer ", " analytics ")):
            scores["Data & Analytics"] += 3
            matched["Data & Analytics"].append("rule:risk title but data function")

    if any(_contains(text, term) for term in (" delivery ", " transformation ", " change management ", " implementation ")):
        scores["Product & Project"] += 3
        matched["Product & Project"].append("rule:delivery/transformation/change")
        scores["Engineering"] -= 2
        matched["Engineering"].append("neg:delivery/transformation")

    if any(_contains(text, term) for term in (" cloud ", " platform ", " security ", " architecture ", " architect ")):
        scores["IT & Infra"] += 3
        matched["IT & Infra"].append("rule:platform/cloud/security/architecture")
        scores["Leadership"] -= 2
        matched["Leadership"].append("neg:architecture title is functional")
        if any(_contains(text, phrase) for phrase in (" data architect ", " data platform ", " data engineering ")):
            scores["Data & Analytics"] += 5
            matched["Data & Analytics"].append("rule:data + platform/architect")

    if any(_contains(text, term) for term in (" strategy analyst ", " strategy manager ", " strategic planning ")):
        if any(_contains(text, term) for term in (" insights ", " analytics ", " research ")):
            scores["Data & Analytics"] += 4
            matched["Data & Analytics"].append("rule:strategy + insights/analytics")
        elif any(_contains(text, term) for term in (" transformation ", " planning ", " execution ")):
            scores["Product & Project"] += 4
            matched["Product & Project"].append("rule:strategy + transformation/planning")

    if any(_contains(text, term) for term in (" support ", " supporting ")):
        if any(_contains(text, term) for term in (" it support ", " desktop support ", " systems support ")):
            scores["IT & Infra"] += 4
            matched["IT & Infra"].append("rule:it/systems support")
        elif any(_contains(text, term) for term in (" customer support ", " client support ", " service representative ")):
            scores["Operations"] += 4
            matched["Operations"].append("rule:client/customer support")
        elif _contains(text, " product support "):
            scores["Product & Project"] += 4
            matched["Product & Project"].append("rule:product support")
            scores["Operations"] += 1
            matched["Operations"].append("rule:product support alt")

    if any(_contains(text, term) for term in (" event ", " career event ", " recruitment event ", " job fair ", " information session ")):
        scores["Other"] += 8
        matched["Other"].append("rule:non-real role / event")

    if any(_contains(text, term) for term in (" release train engineer ", " agile release train ")):
        scores["Product & Project"] += 5
        matched["Product & Project"].append("rule:release train")
        scores["Engineering"] -= 2
        matched["Engineering"].append("neg:release train")

    if any(_contains(text, term) for term in (" workflows ", " workflow ")):
        scores["Product & Project"] += 2
        matched["Product & Project"].append("rule:workflow orchestration")

    if any(_contains(text, term) for term in (" software engineer ", " software engineering ", " developer ", " engineer ")):
        if any(_contains(text, term) for term in (" finance ", " securities ", " capital markets ", " reinsurance ")):
            scores["Engineering"] += 4
            matched["Engineering"].append("rule:engineering terms override finance domain")

    if any(_contains(text, term) for term in (" account manager ", " relationship manager ")):
        if any(_contains(text, term) for term in (" banking ", " business banking ", " real estate finance ", " wealth ", " treasury ")):
            scores["Finance"] += 4
            matched["Finance"].append("rule:finance relationship/account manager")
            scores["Sales & Mktg"] -= 2
            matched["Sales & Mktg"].append("neg:finance relationship/account role")

    if any(_contains(text, term) for term in (" director of finance ", " finance director ", " vice president finance ", " head of finance ", " underwriting ")):
        scores["Finance"] += 4
        matched["Finance"].append("rule:senior finance title")

    if any(_contains(text, term) for term in (" vendor management office ", " maintenance supervisor ", " national accounts ")):
        target = "Operations" if not _contains(text, " national accounts ") else "Sales & Mktg"
        scores[target] += 4
        matched[target].append("rule:domain phrase")

    finance_org = any(org in company for org in COMPANY_HINTS["finance_org"])
    if finance_org and any(_contains(text, term) for term in (" underwriter ", " portfolio ", " credit ", " lending ", " wealth ", " treasury ")):
        scores["Finance"] += 1
        matched["Finance"].append("company:finance_org")
    if finance_org and any(_contains(text, term) for term in (" risk ", " compliance ", " aml ", " audit ")):
        scores["Legal"] += 1
        matched["Legal"].append("company:finance_org")
    if any(org in company for org in COMPANY_HINTS["public_org"]) and any(_contains(text, term) for term in (" nurse ", " clinical ", " therapist ")):
        scores["Other"] += 2
        matched["Other"].append("company:public org non-taxonomy role")

    if any(_contains(text, title) for title in LEADERSHIP_TITLES):
        scores["Leadership"] += 2
        matched["Leadership"].append("title:leadership")
        if any(_contains(text, term) for term in (" vice president ", " head of ", " chief ")):
            scores["Leadership"] += 1
            matched["Leadership"].append("rule:senior leadership title")


def classify_category(job):
    role = str(job.get("role", ""))
    company = str(job.get("company", ""))
    location = str(job.get("location", ""))
    normalized_title = normalize_title(" ".join([role, company, location]))

    scores = {cat: 0 for cat in CATEGORY_PRIORITY}
    matched = {cat: [] for cat in CATEGORY_PRIORITY}

    for category, rules in CATEGORY_RULES.items():
        scores[category] += _score_bucket(normalized_title, rules["strong_phrases"], 5, matched[category], "phrase")
        scores[category] += _score_bucket(normalized_title, rules["strong_tokens"], 3, matched[category], "token")
        scores[category] += _score_bucket(normalized_title, rules["weak_tokens"], 1, matched[category], "weak")
        scores[category] += _score_bucket(normalized_title, rules["negative_phrases"], -4, matched[category], "neg")
        scores[category] += _score_bucket(normalized_title, rules["negative_tokens"], -2, matched[category], "neg")

    _apply_special_cases(job, normalized_title, scores, matched)

    ranked = sorted(
        scores.items(),
        key=lambda item: (-item[1], CATEGORY_PRIORITY.index(item[0])),
    )
    top_category, top_score = ranked[0]
    alt_category, alt_score = ranked[1]

    # Leadership is a late fallback, not the default destination for senior titles.
    if top_category == "Leadership":
        for cat, score in ranked[1:]:
            if cat not in ("Leadership", "Other") and score >= top_score - 1 and score >= 3:
                top_category, top_score = cat, score
                break

    if top_category == "Other":
        for cat, score in ranked[1:]:
            if cat != "Leadership" and score >= 2:
                top_category, top_score = cat, score
                break

    ranked = sorted(
        scores.items(),
        key=lambda item: (-item[1], CATEGORY_PRIORITY.index(item[0])),
    )
    if ranked[0][0] != top_category:
        ranked = [(top_category, scores[top_category])] + [item for item in ranked if item[0] != top_category]
    alt_category, alt_score = next((item for item in ranked[1:] if item[0] != top_category), ("Other", 0))

    gap = top_score - alt_score
    if top_score >= 8 and gap >= 4:
        confidence = "high"
    elif top_score >= 5 and gap >= 2:
        confidence = "medium"
    else:
        confidence = "low"

    if top_score <= 0:
        top_category = "Other"
        confidence = "low"

    return {
        "predicted_category": top_category,
        "confidence_level": confidence,
        "matched_signals": matched.get(top_category, [])[:8],
        "alternative_category_candidate": alt_category if alt_score > 0 else "",
        "normalized_title": normalized_title.strip(),
        "scores": scores,
        "top_score": top_score,
        "alt_score": alt_score,
    }
