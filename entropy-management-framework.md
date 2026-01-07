# Entropy Management Framework for LLM Data Analytics

## Overview

This framework defines a systematic approach to measuring and managing **information entropy** in enterprise data systems. The goal is to quantify the gap between raw data and data that can be used **deterministically** by LLMs to answer natural language queries.

**Core Principle**: Every ambiguity, missing definition, or inconsistency in data represents entropy that reduces an LLM's ability to provide reliable, reproducible answers.

---

## Entropy Dimension Taxonomy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ENTROPY DIMENSIONS                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SOURCE ENTROPY          Data origin and availability                       │
│  ├── Physical            Storage, encoding, format                          │
│  ├── Provenance          Origin, lineage, trust                             │
│  ├── Access Rights       Permissions, visibility, security                  │
│  └── Lifecycle           Freshness, versioning, retention                   │
│                                                                             │
│  STRUCTURAL ENTROPY      Schema and organization                            │
│  ├── Schema              Naming, organization, stability                    │
│  ├── Types               Data type fidelity and consistency                 │
│  └── Relations           Connections between entities                       │
│                                                                             │
│  SEMANTIC ENTROPY        Business meaning and interpretation                │
│  ├── Business Meaning    Definitions, terminology, glossary                 │
│  ├── Units               Measurement units, currencies, scales              │
│  ├── Temporal            Time semantics, accumulation, point-in-time        │
│  └── Categorical         Dimension hierarchies, classification schemes      │
│                                                                             │
│  VALUE ENTROPY           Data quality at column level                       │
│  ├── Categories          Categorical consistency and completeness           │
│  ├── Null Semantics      Missing value interpretation                       │
│  ├── Outliers            Anomalies and exceptional values                   │
│  ├── Ranges              Value boundaries and distributions                 │
│  └── Patterns            Format consistency, regex conformance              │
│                                                                             │
│  COMPUTATIONAL ENTROPY   Derived values and business logic                  │
│  ├── Derived Values      Calculated fields, transformations                 │
│  ├── Business Rules      Domain logic, conditional processing               │
│  ├── Filters             Default exclusions, implicit WHERE clauses         │
│  └── Aggregations        Rollup rules, weighted calculations                │
│                                                                             │
│  QUERY ENTROPY           Natural language interpretation                    │
│  ├── Linguistics         Synonyms, abbreviations, multilingual              │
│  └── Intent              Scope, defaults, output expectations               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Dimension Specifications

Each dimension follows a consistent structure:

- **Question Asked**: What uncertainty does this dimension address?
- **Metrics & Examples**: How is entropy measured? What does high/low entropy look like?
- **Explanation**: Why does this matter for LLM analytics?
- **Entropy Object Structure**: Schema for the metadata object

---

## 1. SOURCE ENTROPY

Source entropy captures uncertainty about where data comes from and whether it can be accessed reliably.

### 1.1 Physical

#### Question Asked
Can the data be read correctly and completely?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `encoding_confidence` | Certainty of character encoding detection | UTF-8 declared, validated | Encoding inferred, special chars present |
| `format_stability` | Consistency of file/data format | Fixed schema, versioned format | Schema inference required, format varies |
| `compression_integrity` | Data completeness after decompression | Checksums valid, full extraction | Partial reads, corruption detected |
| `precision_fidelity` | Numeric representation accuracy | Decimal types, precision declared | Float approximations, precision loss |

**Examples:**
- Low entropy: `encoding: "UTF-8", declared: true, bom_present: true`
- High entropy: CSV with mixed line endings, inferred delimiter, German locale numbers (`1.234,56`)

#### Explanation
Physical entropy is foundational. If data cannot be read correctly, all downstream analysis is compromised. LLMs cannot compensate for corrupted or misencoded data—they will silently produce wrong results.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "source"
  dimension: "physical"
  sub_dimension: "encoding"  # encoding | format | compression | precision
  target: "file:uploads/sales_2024.csv"
  
  score: 0.35  # 0 = deterministic, 1 = maximum uncertainty
  confidence: 0.92
  
  evidence:
    - type: "encoding_detection"
      detected: "UTF-8"
      confidence: 0.94
      method: "chardet"
      risk_characters: ["ü", "ä", "ß"]
    - type: "format_inference"
      delimiter: ","
      delimiter_confidence: 0.98
      quoting: "minimal"
      
  resolution_options:
    - action: "declare_encoding"
      parameters:
        encoding: "UTF-8"
        locale: "de_CH"
      expected_entropy_reduction: 0.30
      effort: "low"
    - action: "validate_and_transcode"
      parameters:
        target_encoding: "UTF-8"
        validation_sample_size: 1000
      expected_entropy_reduction: 0.35
      effort: "medium"
      
  llm_context:
    description: "File encoding detected as UTF-8 with high confidence. German special characters present require locale-aware processing."
    query_impact: "String matching for German names may fail if encoding assumptions are incorrect."
    assumptions_if_unresolved:
      - assumption: "UTF-8 encoding"
        confidence: 0.94
        
  human_context:
    severity: "low"
    category: "Data Ingestion"
    message: "Encoding detected but not declared"
    recommendation: "Add explicit encoding declaration to source configuration"
```

---

### 1.2 Provenance

#### Question Asked
Where did this data come from, and how trustworthy is it?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `source_documented` | Is the data origin recorded? | Source system, extraction method known | Origin unknown or undocumented |
| `lineage_complete` | Can transformations be traced? | Full DAG from source to current | Gaps in transformation history |
| `trust_level` | Reliability of source | System-generated, validated | Manual entry, no validation |
| `reconciliation_status` | Has data been verified? | Reconciled against source | Unverified, potential discrepancies |

**Examples:**
- Low entropy: ERP system extract with audit trail, checksums validated
- High entropy: "sales_data_final_v2_FIXED.xlsx" from email attachment

#### Explanation
Provenance entropy affects how confidently an LLM can assert facts. Data from validated system extracts can be stated definitively; data from manual entry should be hedged. LLMs need this context to calibrate their confidence appropriately.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "source"
  dimension: "provenance"
  sub_dimension: "trust_level"  # source_documented | lineage | trust_level | reconciliation
  target: "table:crm.customer_industry"
  
  score: 0.72
  confidence: 0.85
  
  evidence:
    - type: "source_classification"
      source_type: "manual_entry"
      entry_point: "CRM sales rep input"
      validation_at_entry: false
    - type: "consistency_analysis"
      same_entity_variance: 0.23  # 23% of customers have inconsistent values
      example_inconsistencies:
        - entity: "Customer ABC"
          values_found: ["Tech", "Technology", "Software", "IT Services"]
          
  resolution_options:
    - action: "document_provenance"
      parameters:
        source_system: "Salesforce CRM"
        entry_method: "manual"
        validation_level: "none"
      expected_entropy_reduction: 0.15  # Makes uncertainty explicit
      effort: "low"
    - action: "add_validation_rules"
      parameters:
        validation_type: "picklist"
        allowed_values_source: "industry_taxonomy"
      expected_entropy_reduction: 0.45
      effort: "high"
      
  llm_context:
    description: "Customer industry data is manually entered by sales representatives without validation. Inconsistent classification observed for same customers."
    query_impact: "Industry-based filtering and aggregation unreliable. Results should be presented with confidence caveats."
    trust_level: "low"
    recommended_hedging: "Based on CRM data (manually entered, some inconsistencies observed)..."
    
  human_context:
    severity: "medium"
    category: "Data Quality"
    message: "Manual entry field with no validation"
    recommendation: "Implement picklist validation or external industry enrichment"
```

---

### 1.3 Access Rights

#### Question Asked
Can this data be used for the intended purpose, and by whom?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `permission_clarity` | Are access rules documented? | RBAC defined, documented | Ad-hoc permissions, unclear rules |
| `row_level_security` | Are there implicit filters? | RLS rules explicit | Hidden row filtering |
| `pii_classification` | Is sensitive data identified? | PII tagged, handling defined | Potential PII unclassified |
| `usage_restrictions` | Are there legal/contractual limits? | Data use agreement clear | Unknown restrictions |

**Examples:**
- Low entropy: Column marked `pii: true, handling: "mask_for_non_admin"`
- High entropy: Customer table with unclear GDPR implications

#### Explanation
Access entropy affects what queries can be answered and how. If an LLM doesn't know about row-level security, it may give different users the same answer when they should see different data. PII handling affects whether data can be shown in responses.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "source"
  dimension: "access_rights"
  sub_dimension: "row_level_security"  # permission_clarity | rls | pii | usage_restrictions
  target: "table:sales.opportunities"
  
  score: 0.58
  confidence: 0.78
  
  evidence:
    - type: "rls_detection"
      rls_present: true
      rls_documented: false
      inferred_filter: "WHERE owner_id = current_user_id OR is_admin(current_user)"
    - type: "query_variance"
      description: "Same query returns different row counts for different users"
      example:
        user_a_count: 1247
        user_b_count: 3891
        
  resolution_options:
    - action: "document_rls_rules"
      parameters:
        rule_description: "Users see only their owned opportunities unless admin"
        filter_column: "owner_id"
      expected_entropy_reduction: 0.40
      effort: "medium"
      
  llm_context:
    description: "Row-level security is active on opportunities table but rules are not documented. Query results vary by user context."
    query_impact: "Aggregations like 'total opportunities' will differ by user. LLM must clarify scope or indicate result is user-filtered."
    context_required: ["current_user_id", "user_role"]
    
  human_context:
    severity: "medium"
    category: "Security & Access"
    message: "Undocumented row-level security"
    recommendation: "Document RLS rules in metadata for query-time context injection"
```

---

### 1.4 Lifecycle

#### Question Asked
Is this data current, and what is its temporal validity?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `freshness` | When was data last updated? | Timestamp known, SLA defined | Unknown last update |
| `update_frequency` | How often does data change? | Documented refresh schedule | Irregular, unpredictable updates |
| `version_clarity` | Which version is authoritative? | Single source of truth | Multiple versions, unclear which is current |
| `retention_policy` | How long is data kept? | Policy documented | Unknown retention, potential gaps |

**Examples:**
- Low entropy: `last_updated: "2024-01-15T08:00:00Z", refresh_schedule: "daily 06:00 UTC"`
- High entropy: Static export from 6 months ago, no update tracking

#### Explanation
Lifecycle entropy determines whether an LLM can answer "current state" questions. If freshness is unknown, the LLM cannot distinguish between "this is definitely true now" and "this was true at some point."

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "source"
  dimension: "lifecycle"
  sub_dimension: "freshness"  # freshness | update_frequency | version | retention
  target: "table:inventory.stock_levels"
  
  score: 0.45
  confidence: 0.90
  
  evidence:
    - type: "freshness_detection"
      last_modified: "2024-01-14T06:00:00Z"
      detection_method: "table_metadata"
      age_hours: 26
    - type: "update_pattern_analysis"
      detected_pattern: "daily"
      pattern_confidence: 0.85
      last_expected_update: "2024-01-15T06:00:00Z"
      status: "overdue"
      
  resolution_options:
    - action: "document_refresh_schedule"
      parameters:
        schedule: "daily 06:00 UTC"
        sla_max_delay_hours: 2
      expected_entropy_reduction: 0.35
      effort: "low"
    - action: "add_freshness_monitoring"
      parameters:
        alert_threshold_hours: 4
        staleness_indicator_column: "_data_freshness_status"
      expected_entropy_reduction: 0.40
      effort: "medium"
      
  llm_context:
    description: "Stock levels table was last updated 26 hours ago. Expected daily refresh appears overdue."
    query_impact: "Current inventory queries may be stale. Consider adding 'as of' qualifier to responses."
    staleness_warning: "Data may be up to 26 hours old"
    
  human_context:
    severity: "medium"
    category: "Data Currency"
    message: "Data refresh appears overdue"
    recommendation: "Verify refresh pipeline status; document expected schedule"
```

---

## 2. STRUCTURAL ENTROPY

Structural entropy captures uncertainty about how data is organized and interconnected.

### 2.1 Schema

#### Question Asked
Can the data structure be interpreted unambiguously?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `naming_clarity` | Are identifiers self-explanatory? | `customer_lifetime_value_usd` | `clv`, `c_val`, `amt1` |
| `naming_consistency` | Do conventions apply uniformly? | Consistent snake_case throughout | Mixed conventions, inconsistent prefixes |
| `schema_stability` | How often does structure change? | Versioned, backward compatible | Frequent breaking changes |
| `organization_logic` | Is table/column grouping intuitive? | Clear domain separation | Arbitrary organization |

**Examples:**
- Low entropy: `orders.customer_id`, `orders.order_date`, `orders.total_amount_usd`
- High entropy: `tbl1.c_id`, `tbl1.dt`, `tbl1.amt`

#### Explanation
Schema entropy directly affects LLM query generation. Ambiguous names force the LLM to guess which column to use. Clear naming enables confident, deterministic SQL generation.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "structural"
  dimension: "schema"
  sub_dimension: "naming_clarity"  # naming_clarity | naming_consistency | stability | organization
  target: "column:fin_data.c_amt"
  
  score: 0.68
  confidence: 0.88
  
  evidence:
    - type: "abbreviation_detected"
      original_name: "c_amt"
      possible_expansions:
        - expansion: "customer_amount"
          confidence: 0.25
        - expansion: "credit_amount"
          confidence: 0.35
        - expansion: "current_amount"
          confidence: 0.20
        - expansion: "contract_amount"
          confidence: 0.20
    - type: "context_analysis"
      table_context: "fin_data appears to be financial data"
      sibling_columns: ["c_id", "c_dt", "c_typ"]
      pattern: "consistent 'c_' prefix but meaning unclear"
      
  resolution_options:
    - action: "add_column_alias"
      parameters:
        alias: "credit_amount"
        description: "Credit amount in transaction currency"
      expected_entropy_reduction: 0.55
      effort: "low"
    - action: "rename_column"
      parameters:
        new_name: "credit_amount_local_currency"
      expected_entropy_reduction: 0.65
      effort: "medium"
      breaking_change: true
      
  llm_context:
    description: "Column 'c_amt' uses abbreviated naming. Most likely 'credit_amount' based on table context, but could be customer, current, or contract amount."
    query_impact: "Queries referencing 'amount' are ambiguous. Multiple candidate columns exist."
    disambiguation_required: true
    best_guess: "credit_amount"
    best_guess_confidence: 0.35
    
  human_context:
    severity: "high"
    category: "Schema Design"
    message: "Ambiguous column name"
    recommendation: "Add descriptive alias or rename to self-documenting name"
```

---

### 2.2 Types

#### Question Asked
Do declared types accurately represent the data?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `type_fidelity` | Does declared type match content? | DATE column contains dates | DATE stored as VARCHAR |
| `type_consistency` | Same concept, same type across tables? | `customer_id` always INTEGER | Sometimes INTEGER, sometimes VARCHAR |
| `nullability_accuracy` | Does nullable declaration match reality? | NOT NULL has no nulls | NOT NULL contains nulls |
| `precision_appropriateness` | Is numeric precision sufficient? | DECIMAL(10,2) for currency | FLOAT for financial amounts |

**Examples:**
- Low entropy: `order_date DATE NOT NULL`, all values are valid dates
- High entropy: `order_date VARCHAR(50)` containing "2024-01-15", "Jan 15, 2024", "15.01.2024", ""

#### Explanation
Type entropy affects query correctness. If dates are stored as strings, date arithmetic fails or produces wrong results. LLMs need type information to generate correct SQL and interpret results.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "structural"
  dimension: "types"
  sub_dimension: "type_fidelity"  # type_fidelity | type_consistency | nullability | precision
  target: "column:events.event_date"
  
  score: 0.72
  confidence: 0.95
  
  evidence:
    - type: "type_mismatch"
      declared_type: "VARCHAR(50)"
      detected_content_type: "date"
      format_variance:
        - format: "YYYY-MM-DD"
          frequency: 0.65
        - format: "DD.MM.YYYY"
          frequency: 0.25
        - format: "Mon DD, YYYY"
          frequency: 0.08
        - format: "empty_string"
          frequency: 0.02
    - type: "parsing_risk"
      ambiguous_values:
        - value: "01.02.2024"
          interpretations: ["Jan 2, 2024", "Feb 1, 2024"]
          
  resolution_options:
    - action: "convert_to_proper_type"
      parameters:
        target_type: "DATE"
        parsing_format: "auto_detect"
        null_handling: "empty_to_null"
      expected_entropy_reduction: 0.65
      effort: "medium"
    - action: "add_type_annotation"
      parameters:
        semantic_type: "date"
        expected_formats: ["YYYY-MM-DD", "DD.MM.YYYY"]
        locale: "de_CH"
      expected_entropy_reduction: 0.45
      effort: "low"
      
  llm_context:
    description: "Column 'event_date' is stored as VARCHAR but contains date values in multiple formats. Some dates are ambiguous (e.g., '01.02.2024' could be January 2 or February 1)."
    query_impact: "Date filtering and arithmetic will fail or produce incorrect results. Date comparisons as strings will sort incorrectly."
    workaround: "Parse with explicit format specification; flag ambiguous dates"
    
  human_context:
    severity: "high"
    category: "Data Types"
    message: "Date stored as string with multiple formats"
    recommendation: "Convert to DATE type with explicit parsing rules"
```

---

### 2.3 Relations

#### Question Asked
How do entities connect, and which connections are authoritative?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `cardinality_clarity` | Is relationship multiplicity defined? | FK with documented 1:N | Implicit join, cardinality unknown |
| `join_path_determinism` | Is there one correct path between entities? | Single canonical path | Multiple valid paths with different semantics |
| `referential_integrity` | Do foreign keys resolve? | All FKs valid | Orphaned references exist |
| `relationship_semantics` | Is the meaning of connection clear? | "customer placed order" | Generic "related_to" |

**Examples:**
- Low entropy: `orders.customer_id REFERENCES customers.id` (1:N documented)
- High entropy: `orders` joins to `products` via `line_items` OR `recommendations`—which is correct?

#### Explanation
Relational entropy is where LLM analytics most commonly fail silently. The LLM picks a join path that's syntactically valid but semantically wrong, producing plausible-looking but incorrect results.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "structural"
  dimension: "relations"
  sub_dimension: "join_path_determinism"  # cardinality | join_paths | referential_integrity | semantics
  target: "relationship:orders-to-products"
  
  score: 0.62
  confidence: 0.85
  
  evidence:
    - type: "multiple_paths_detected"
      paths:
        - path: ["orders", "order_items", "products"]
          semantics: "products actually purchased"
          join_keys: ["orders.id = order_items.order_id", "order_items.product_id = products.id"]
        - path: ["orders", "recommendations", "products"]
          semantics: "products recommended at order time"
          join_keys: ["orders.id = recommendations.order_id", "recommendations.product_id = products.id"]
    - type: "no_canonical_declaration"
      description: "No primary relationship designated between orders and products"
      
  resolution_options:
    - action: "declare_canonical_relationship"
      parameters:
        primary_path: ["orders", "order_items", "products"]
        relationship_name: "purchased_products"
        description: "Products that were actually ordered"
      expected_entropy_reduction: 0.50
      effort: "low"
    - action: "create_semantic_views"
      parameters:
        views:
          - name: "order_purchased_products"
            path: ["orders", "order_items", "products"]
          - name: "order_recommended_products"
            path: ["orders", "recommendations", "products"]
      expected_entropy_reduction: 0.58
      effort: "medium"
      
  llm_context:
    description: "Multiple valid join paths exist between orders and products: through order_items (purchased) or recommendations (suggested). Query 'products for order X' is ambiguous."
    query_impact: "Different paths produce different results. LLM must either ask for clarification or clearly state which interpretation was used."
    default_path: ["orders", "order_items", "products"]
    default_interpretation: "purchased products"
    default_confidence: 0.70
    
  human_context:
    severity: "high"
    category: "Data Model"
    message: "Ambiguous join paths"
    recommendation: "Declare canonical relationship or create disambiguating semantic views"
```

---

## 3. SEMANTIC ENTROPY

Semantic entropy captures uncertainty about business meaning and interpretation.

### 3.1 Business Meaning

#### Question Asked
Is the business definition of this data element clear and documented?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `definition_exists` | Is there a documented definition? | Glossary entry with examples | No definition |
| `definition_precision` | Is the definition unambiguous? | "Revenue: invoiced amount excluding tax" | "Revenue: money from sales" |
| `cross_reference_consistency` | Same term, same meaning everywhere? | Consistent across systems | Different definitions per department |
| `example_availability` | Are concrete examples provided? | Sample values with interpretation | Definition only, no examples |

**Examples:**
- Low entropy: "Gross Margin = (Revenue - COGS) / Revenue, expressed as percentage, calculated monthly"
- High entropy: Column named "margin" with no definition

#### Explanation
Business meaning entropy is often the highest-impact dimension. An undefined "margin" could be gross, net, or contribution margin—each with materially different values. LLMs cannot answer "why is margin declining?" without knowing which margin.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "semantic"
  dimension: "business_meaning"
  sub_dimension: "definition_precision"  # definition_exists | precision | consistency | examples
  target: "column:financials.margin"
  
  score: 0.78
  confidence: 0.90
  
  evidence:
    - type: "no_definition"
      description: "Column has no business definition in metadata"
    - type: "term_ambiguity"
      term: "margin"
      possible_meanings:
        - meaning: "gross_margin"
          formula: "(revenue - cogs) / revenue"
          typical_range: "0.20 - 0.60"
        - meaning: "net_margin"
          formula: "(revenue - all_costs) / revenue"
          typical_range: "0.05 - 0.20"
        - meaning: "contribution_margin"
          formula: "(revenue - variable_costs) / revenue"
          typical_range: "0.30 - 0.70"
    - type: "value_range_analysis"
      observed_range: [0.15, 0.45]
      mean: 0.32
      interpretation: "Consistent with gross_margin or contribution_margin, unlikely net_margin"
      
  resolution_options:
    - action: "add_definition"
      parameters:
        definition: "Gross margin: (revenue - cogs) / revenue"
        formula: "(revenue - cogs) / revenue"
        unit: "percentage"
        examples:
          - scenario: "Product A: Revenue $100, COGS $60"
            result: "Margin = 0.40 or 40%"
      expected_entropy_reduction: 0.70
      effort: "low"
    - action: "link_to_glossary"
      parameters:
        glossary_term_id: "fin_gross_margin"
      expected_entropy_reduction: 0.65
      effort: "low"
      
  llm_context:
    description: "Column 'margin' has no definition. Could be gross margin, net margin, or contribution margin. Value distribution suggests gross or contribution margin."
    query_impact: "Cannot explain margin changes or compare to benchmarks without knowing which margin type."
    best_guess: "gross_margin"
    best_guess_confidence: 0.55
    assumption_disclosure: "Assuming gross margin based on value range; verify with business owner"
    
  human_context:
    severity: "critical"
    category: "Business Definitions"
    message: "Undefined financial metric"
    recommendation: "Add formula and definition to glossary; link column to glossary entry"
```

---

### 3.2 Units

#### Question Asked
What unit of measurement applies to this value?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `unit_declared` | Is the unit explicitly stated? | `amount_eur`, metadata: EUR | `amount` with no unit info |
| `unit_consistency` | Same unit throughout column? | All values in EUR | Mixed EUR/CHF/USD |
| `conversion_available` | Can values be converted to common unit? | Exchange rates available | No conversion mechanism |
| `scale_clarity` | Is the magnitude clear? | "thousands", "millions" declared | Unclear if raw or scaled |

**Examples:**
- Low entropy: `revenue_eur DECIMAL(12,2)` with metadata `unit: EUR, scale: 1`
- High entropy: `revenue` column, unknown currency, possibly in thousands

#### Explanation
Unit entropy causes silent calculation errors. Summing EUR and CHF produces meaningless numbers. Comparing values where one is in thousands creates 1000x errors. LLMs need units to generate correct arithmetic and comparisons.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "semantic"
  dimension: "units"
  sub_dimension: "unit_declared"  # unit_declared | consistency | conversion | scale
  target: "column:sales.revenue"
  
  score: 0.65
  confidence: 0.82
  
  evidence:
    - type: "no_unit_declaration"
      description: "Column has no unit metadata"
    - type: "value_pattern_analysis"
      observed_patterns:
        - pattern: "large_numbers"
          sample: [1234567.89, 987654.32]
          interpretation: "Could be raw currency or thousands"
        - pattern: "decimal_precision"
          precision: 2
          interpretation: "Suggests currency, not thousands"
    - type: "context_inference"
      table_name: "sales"
      company_context: "Swiss company"
      likely_currency: ["CHF", "EUR"]
      confidence: 0.60
      
  resolution_options:
    - action: "declare_unit"
      parameters:
        unit: "CHF"
        scale: 1
        precision: 2
      expected_entropy_reduction: 0.60
      effort: "low"
    - action: "rename_with_unit"
      parameters:
        new_name: "revenue_chf"
      expected_entropy_reduction: 0.55
      effort: "medium"
      
  llm_context:
    description: "Column 'revenue' has no declared unit. Context suggests Swiss Francs (CHF) or Euros (EUR)."
    query_impact: "Currency conversions impossible. Cross-table comparisons may be meaningless if currencies differ."
    assumption_if_unresolved: "Assuming CHF based on company context"
    assumption_confidence: 0.60
    
  human_context:
    severity: "high"
    category: "Units & Measurement"
    message: "Currency not specified"
    recommendation: "Add unit declaration; consider renaming column to include currency"
```

---

### 3.3 Temporal

#### Question Asked
What time semantics apply to this value?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `accumulation_type` | Is this a stock or flow? | `type: "ytd_cumulative"` | Could be period value or running total |
| `point_in_time_clarity` | When is this value valid? | `as_of_date` column present | Snapshot date unclear |
| `timezone_handling` | Are time zones explicit? | All UTC, documented | Mixed or unspecified timezones |
| `period_alignment` | What period does value represent? | "calendar_month" documented | Period boundaries unclear |

**Examples:**
- Low entropy: `revenue_ytd` with metadata `accumulation: "year_to_date", reset: "calendar_year"`
- High entropy: `total_revenue` that might be YTD, TTM, or all-time

#### Explanation
Temporal entropy causes subtle but significant errors. If "total_revenue" is YTD cumulative, answering "Q3 revenue" requires calculating a delta. If it's periodic, direct filtering works. LLMs cannot know which approach without temporal semantics.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "semantic"
  dimension: "temporal"
  sub_dimension: "accumulation_type"  # accumulation | point_in_time | timezone | period_alignment
  target: "column:financials.total_revenue"
  
  score: 0.58
  confidence: 0.75
  
  evidence:
    - type: "accumulation_pattern_detected"
      analysis:
        monotonic_increase: true
        resets_detected:
          - date: "2024-01-01"
          - date: "2023-01-01"
        reset_pattern: "calendar_year_start"
        interpretation: "Likely YTD cumulative"
        confidence: 0.72
    - type: "naming_ambiguity"
      name: "total_revenue"
      possible_interpretations:
        - "YTD cumulative"
        - "all-time total"
        - "period total (monthly)"
        
  resolution_options:
    - action: "declare_temporal_semantics"
      parameters:
        accumulation_type: "ytd_cumulative"
        reset_period: "calendar_year"
        as_of_semantics: "end_of_period"
      expected_entropy_reduction: 0.50
      effort: "low"
    - action: "add_period_column"
      parameters:
        column_name: "monthly_revenue"
        derivation: "delta from previous month's total_revenue"
      expected_entropy_reduction: 0.45
      effort: "medium"
      
  llm_context:
    description: "Column 'total_revenue' shows YTD cumulative pattern based on detected resets at year boundaries. Not explicitly declared."
    query_impact: "Queries for specific periods (Q3, monthly) require delta calculations if truly cumulative."
    assumed_semantics: "YTD cumulative"
    assumption_confidence: 0.72
    calculation_note: "For period values, calculate: current_total - previous_period_total"
    
  human_context:
    severity: "medium"
    category: "Temporal Semantics"
    message: "Accumulation type inferred but not declared"
    recommendation: "Add explicit temporal semantics annotation"
```

---

### 3.4 Categorical

#### Question Asked
Are dimension hierarchies and classification schemes well-defined?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `hierarchy_definition` | Are parent-child relationships clear? | `region > country > city` documented | Flat categories, no hierarchy |
| `level_semantics` | What does each level represent? | Each level has clear meaning | Arbitrary groupings |
| `completeness` | Does hierarchy cover all values? | All values mapped | Orphan values exist |
| `stability` | How often does hierarchy change? | Versioned, change-tracked | Frequent undocumented changes |

**Examples:**
- Low entropy: Product hierarchy with defined levels: Category > Subcategory > Product Line > SKU
- High entropy: "department" column with values that sometimes mean org unit, sometimes cost center

#### Explanation
Categorical entropy affects aggregation and drill-down. If hierarchy is unclear, "sales by region" might produce wrong rollups. LLMs need hierarchy definitions to correctly group and aggregate.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "semantic"
  dimension: "categorical"
  sub_dimension: "hierarchy_definition"  # hierarchy | level_semantics | completeness | stability
  target: "dimension:product_category"
  
  score: 0.52
  confidence: 0.80
  
  evidence:
    - type: "hierarchy_detection"
      detected_levels:
        - level: "category"
          distinct_values: 12
          examples: ["Electronics", "Apparel", "Home"]
        - level: "subcategory"
          distinct_values: 89
          examples: ["Phones", "Laptops", "T-Shirts"]
        - level: "product_line"
          distinct_values: 340
      relationship_inferred: "parent_child based on naming patterns"
      confidence: 0.75
    - type: "orphan_detection"
      orphan_count: 23
      examples:
        - value: "Misc"
          count: 156
          interpretation: "Catch-all category"
          
  resolution_options:
    - action: "define_hierarchy"
      parameters:
        hierarchy_name: "product_hierarchy"
        levels:
          - name: "category"
            description: "Top-level product grouping"
          - name: "subcategory"
            description: "Second-level grouping within category"
          - name: "product_line"
            description: "Specific product line within subcategory"
      expected_entropy_reduction: 0.40
      effort: "medium"
    - action: "resolve_orphans"
      parameters:
        orphan_handling: "create_unmapped_category"
      expected_entropy_reduction: 0.15
      effort: "low"
      
  llm_context:
    description: "Product hierarchy detected with 3 levels (category > subcategory > product_line). Hierarchy is inferred from data patterns, not explicitly defined. 23 products in catch-all 'Misc' category."
    query_impact: "Rollup queries may miss orphaned products. Drill-down paths are assumed, not guaranteed."
    hierarchy_assumed: ["category", "subcategory", "product_line"]
    
  human_context:
    severity: "medium"
    category: "Dimensional Model"
    message: "Hierarchy inferred but not documented"
    recommendation: "Formalize hierarchy definition; resolve orphan categorization"
```

---

## 4. VALUE ENTROPY

Value entropy captures data quality issues at the column level—the actual content rather than its meaning.

### 4.1 Categories

#### Question Asked
Are categorical values consistent and complete?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `value_consistency` | Same concept, same encoding? | "Active" always "Active" | "Active", "active", "A", "1" |
| `vocabulary_completeness` | Are all valid values documented? | Enum with all values listed | Unknown valid values |
| `variant_density` | How many variants per concept? | 1 variant per concept | Multiple spellings/encodings |
| `coverage` | What % of values are recognized? | 100% match known values | Many unknown values |

**Examples:**
- Low entropy: Status column with enum: ["Active", "Inactive", "Pending"]
- High entropy: Status column with values: "Active", "active", "ACTIVE", "A", "Y", "1", "true"

#### Explanation
Category entropy causes filtering and grouping failures. "WHERE status = 'Active'" misses "active" and "A". Aggregations produce fragmented results. LLMs need consistent categories or explicit mappings.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "value"
  dimension: "categories"
  sub_dimension: "value_consistency"  # consistency | vocabulary | variants | coverage
  target: "column:customers.status"
  
  score: 0.67
  confidence: 0.92
  
  evidence:
    - type: "variant_detection"
      concept_groups:
        - concept: "active"
          variants: ["Active", "active", "ACTIVE", "A", "Y", "1"]
          count: 8934
        - concept: "inactive"
          variants: ["Inactive", "inactive", "N", "0"]
          count: 2341
        - concept: "unknown"
          variants: ["", null, "?", "TBD"]
          count: 156
    - type: "consistency_score"
      unique_values: 14
      distinct_concepts: 3
      variant_ratio: 4.67  # variants per concept
      
  resolution_options:
    - action: "create_normalization_mapping"
      parameters:
        mappings:
          - from: ["Active", "active", "ACTIVE", "A", "Y", "1"]
            to: "active"
          - from: ["Inactive", "inactive", "N", "0"]
            to: "inactive"
          - from: ["", null, "?", "TBD"]
            to: null
      expected_entropy_reduction: 0.60
      effort: "medium"
    - action: "add_canonical_column"
      parameters:
        column_name: "status_normalized"
        source_column: "status"
        apply_mapping: true
      expected_entropy_reduction: 0.55
      effort: "medium"
      
  llm_context:
    description: "Status column has 14 distinct values representing 3 concepts. Multiple variants exist for 'active' (6 forms) and 'inactive' (4 forms)."
    query_impact: "Simple filters like 'WHERE status = \"Active\"' will miss most active records. Must use LOWER() or IN clauses."
    filter_recommendation: "WHERE LOWER(status) IN ('active', 'a', 'y', '1')"
    
  human_context:
    severity: "high"
    category: "Data Consistency"
    message: "Categorical values have multiple variants"
    recommendation: "Implement normalization mapping or add normalized column"
```

---

### 4.2 Null Semantics

#### Question Asked
What does a missing value mean in this context?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `null_meaning_defined` | Is null interpretation documented? | "NULL = not applicable" | No definition |
| `null_consistency` | Does null always mean the same thing? | Consistent meaning | Context-dependent |
| `null_representation` | How are missing values encoded? | SQL NULL only | NULL, "", "N/A", "none", 0 |
| `null_handling_rules` | How should nulls be processed? | Exclude from avg, include in count | No rules defined |

**Examples:**
- Low entropy: `discount_percent NULL` means "no discount" (treat as 0 for calculations)
- High entropy: `manager_id NULL` could mean "no manager", "unknown manager", or "not yet assigned"

#### Explanation
Null semantics entropy causes aggregation errors. Should NULL discounts be excluded from averages or treated as zero? Different interpretations produce different numbers. LLMs need explicit null handling rules.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "value"
  dimension: "null_semantics"
  sub_dimension: "null_meaning"  # meaning | consistency | representation | handling_rules
  target: "column:orders.discount_percent"
  
  score: 0.48
  confidence: 0.85
  
  evidence:
    - type: "null_frequency"
      null_count: 4521
      total_count: 15000
      null_rate: 0.30
    - type: "null_representation_variants"
      representations:
        - value: null
          count: 4200
        - value: ""
          count: 250
        - value: "0"
          count: 71
          interpretation: "Ambiguous: zero discount or null?"
    - type: "semantic_ambiguity"
      possible_meanings:
        - meaning: "no discount offered"
          treatment: "treat as 0"
        - meaning: "discount unknown"
          treatment: "exclude from calculations"
        - meaning: "discount not applicable"
          treatment: "exclude from calculations"
          
  resolution_options:
    - action: "define_null_semantics"
      parameters:
        meaning: "no discount offered"
        calculation_treatment: "treat_as_zero"
        display_treatment: "show_as_dash"
      expected_entropy_reduction: 0.40
      effort: "low"
    - action: "normalize_representations"
      parameters:
        null_variants: ["", "N/A", "none"]
        target: null
      expected_entropy_reduction: 0.25
      effort: "low"
      
  llm_context:
    description: "30% of discount_percent values are null or empty. Unclear if null means 'no discount' (treat as 0) or 'unknown discount' (exclude from calculations)."
    query_impact: "Average discount calculation could be 15% (excluding nulls) or 10.5% (treating nulls as 0)."
    assumption_if_unresolved: "Treating NULL as 0 (no discount)"
    assumption_confidence: 0.65
    
  human_context:
    severity: "medium"
    category: "Null Handling"
    message: "Null semantics undefined"
    recommendation: "Document null meaning and add handling rules to metadata"
```

---

### 4.3 Outliers

#### Question Asked
Are there anomalous values that might indicate errors or special cases?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `outlier_rate` | What % of values are outliers? | < 1%, all explained | > 5%, unexplained |
| `outlier_documentation` | Are outliers explained? | "Values > 1M are bulk orders" | No documentation |
| `outlier_handling` | How should outliers be treated? | Documented inclusion/exclusion rules | No guidance |
| `detection_method` | How were outliers identified? | IQR, Z-score documented | Not analyzed |

**Examples:**
- Low entropy: Order values > $100K flagged, documented as "enterprise deals, exclude from standard metrics"
- High entropy: Revenue column with values ranging from $1 to $50M, no explanation for extremes

#### Explanation
Outlier entropy affects aggregation reliability. A single $50M order in otherwise $500-average data skews all means. LLMs need to know whether to include, exclude, or separately report outliers.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "value"
  dimension: "outliers"
  sub_dimension: "outlier_handling"  # rate | documentation | handling | detection
  target: "column:orders.order_value"
  
  score: 0.55
  confidence: 0.88
  
  evidence:
    - type: "outlier_detection"
      method: "iqr"
      iqr_multiplier: 1.5
      lower_bound: 45.00
      upper_bound: 2500.00
      outlier_count: 234
      outlier_rate: 0.023
      max_outlier: 4500000.00
    - type: "outlier_impact"
      mean_with_outliers: 847.32
      mean_without_outliers: 312.45
      impact_ratio: 2.71  # Outliers nearly triple the mean
    - type: "outlier_patterns"
      patterns:
        - pattern: "enterprise_orders"
          threshold: "> 100000"
          count: 45
          interpretation: "Likely B2B enterprise deals"
          
  resolution_options:
    - action: "document_outlier_policy"
      parameters:
        classification:
          - range: "> 100000"
            label: "enterprise_order"
            handling: "exclude_from_standard_metrics"
          - range: "< 10"
            label: "sample_or_test"
            handling: "exclude_from_analysis"
      expected_entropy_reduction: 0.45
      effort: "medium"
    - action: "add_outlier_flag"
      parameters:
        column_name: "is_outlier"
        detection_method: "iqr_1.5"
      expected_entropy_reduction: 0.35
      effort: "low"
      
  llm_context:
    description: "Order values have outliers (2.3% of records) that significantly impact aggregations. Mean is $847 with outliers, $312 without. Maximum value is $4.5M."
    query_impact: "Average order value queries will be skewed. Consider median or trimmed mean, or report with/without enterprise orders."
    recommendation: "Report as 'Average order value: $312 (excluding enterprise orders)' or provide both figures"
    
  human_context:
    severity: "medium"
    category: "Data Distribution"
    message: "Significant outliers affecting aggregations"
    recommendation: "Define outlier handling policy; consider separate enterprise reporting"
```

---

### 4.4 Ranges

#### Question Asked
Are value boundaries known and enforced?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `bounds_defined` | Are min/max documented? | `percentage: 0-100` | No bounds declared |
| `bounds_enforced` | Do values respect bounds? | All values in range | Out-of-range values exist |
| `distribution_expected` | Is expected distribution known? | "Normal, mean ~50" | No distribution info |
| `boundary_semantics` | What do bounds mean? | Hard limits vs soft guidelines | Unclear |

**Examples:**
- Low entropy: `satisfaction_score` declared 1-5, all values in range
- High entropy: `percentage` column with values ranging from -15 to 350

#### Explanation
Range entropy causes validation and interpretation issues. A percentage of 350% might be valid (growth rate) or an error (should be 35.0%). LLMs need range context to validate results and flag anomalies.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "value"
  dimension: "ranges"
  sub_dimension: "bounds_enforced"  # bounds_defined | enforced | distribution | boundary_semantics
  target: "column:products.margin_percent"
  
  score: 0.42
  confidence: 0.90
  
  evidence:
    - type: "range_analysis"
      observed_min: -25.5
      observed_max: 185.3
      expected_range: [0, 100]  # For typical percentage
      out_of_range_count: 89
    - type: "boundary_violations"
      violations:
        - type: "negative_values"
          count: 34
          sample: [-25.5, -12.3, -5.0]
          possible_meaning: "Loss-making products"
        - type: "over_100"
          count: 55
          sample: [125.0, 150.0, 185.3]
          possible_meaning: "Calculation error or different metric"
          
  resolution_options:
    - action: "define_valid_range"
      parameters:
        expected_min: -100
        expected_max: 100
        semantic_note: "Negative values indicate loss; range is -100% to 100%"
      expected_entropy_reduction: 0.30
      effort: "low"
    - action: "investigate_violations"
      parameters:
        flag_column: "margin_needs_review"
        criteria: "margin_percent > 100"
      expected_entropy_reduction: 0.25
      effort: "medium"
      
  llm_context:
    description: "Margin percentage has values outside typical 0-100% range. Negative values (-25% to 0) may indicate loss-makers. Values over 100% (up to 185%) need investigation."
    query_impact: "Aggregations may include anomalous values. Filter recommendations should account for data quality."
    validation_note: "Flag products with margin > 100% as potentially erroneous"
    
  human_context:
    severity: "medium"
    category: "Data Validation"
    message: "Values outside expected range"
    recommendation: "Define valid range; investigate out-of-range values"
```

---

### 4.5 Patterns

#### Question Asked
Do values conform to expected formats and patterns?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `format_consistency` | Do values follow same pattern? | All emails match regex | Mixed formats |
| `pattern_documentation` | Are expected patterns declared? | Regex or format string provided | No pattern info |
| `conformance_rate` | What % match expected pattern? | > 99% | < 90% |
| `variant_handling` | How to treat non-conforming values? | Documented handling | No guidance |

**Examples:**
- Low entropy: `phone_number` all match `+41 ## ### ## ##`
- High entropy: `phone_number` with values like "044 123 45 67", "+41441234567", "0041-44-123-45-67"

#### Explanation
Pattern entropy affects parsing and matching. Phone numbers in different formats can't be easily compared or deduplicated. LLMs need pattern information to correctly parse, validate, and query string data.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "value"
  dimension: "patterns"
  sub_dimension: "format_consistency"  # consistency | documentation | conformance | variant_handling
  target: "column:contacts.phone_number"
  
  score: 0.58
  confidence: 0.85
  
  evidence:
    - type: "pattern_detection"
      detected_patterns:
        - pattern: "+## ## ### ## ##"
          regex: "\\+\\d{2} \\d{2} \\d{3} \\d{2} \\d{2}"
          frequency: 0.45
          example: "+41 44 123 45 67"
        - pattern: "0## ### ## ##"
          regex: "0\\d{2} \\d{3} \\d{2} \\d{2}"
          frequency: 0.35
          example: "044 123 45 67"
        - pattern: "unstructured"
          frequency: 0.20
          examples: ["441234567", "44-123-45-67", "44 123 4567"]
    - type: "normalization_potential"
      can_normalize: true
      target_format: "E.164"
      confidence: 0.75
      
  resolution_options:
    - action: "normalize_pattern"
      parameters:
        target_format: "E.164"
        example: "+41441234567"
        normalization_rules:
          - from_pattern: "0(\\d{2}) (\\d{3}) (\\d{2}) (\\d{2})"
            to_pattern: "+41$1$2$3$4"
      expected_entropy_reduction: 0.50
      effort: "medium"
    - action: "add_pattern_annotation"
      parameters:
        expected_patterns: ["E.164", "Swiss national"]
        validation_regex: "^(\\+41|0)\\d{9,10}$"
      expected_entropy_reduction: 0.30
      effort: "low"
      
  llm_context:
    description: "Phone numbers appear in multiple formats (international +41, national 0xx, unstructured). Matching and deduplication require normalization."
    query_impact: "Searching for a phone number may require pattern-aware matching. Exact string match will miss variants."
    search_recommendation: "Normalize to digits-only or use pattern-aware search"
    
  human_context:
    severity: "medium"
    category: "Data Formats"
    message: "Multiple format variants detected"
    recommendation: "Add normalization step or document expected patterns"
```

---

## 5. COMPUTATIONAL ENTROPY

Computational entropy captures uncertainty about how values are calculated and combined.

### 5.1 Derived Values

#### Question Asked
How are calculated fields produced?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `formula_documented` | Is calculation defined? | SQL/formula provided | "Just a number" |
| `reproducibility` | Can value be recalculated? | Yes, from source columns | No, inputs unknown |
| `component_traceability` | Are inputs identifiable? | Clear lineage to source | Black box |
| `calculation_timing` | When is value computed? | "Nightly batch" documented | Unknown refresh |

**Examples:**
- Low entropy: `cltv = avg_order_value * order_frequency * customer_lifespan`
- High entropy: `health_score` column with no formula, no explanation

#### Explanation
Derived value entropy prevents explanation and validation. If an LLM can't explain how a metric is calculated, it can't answer "why did X change?" or validate results. Formulas enable debugging and trust.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "computational"
  dimension: "derived_values"
  sub_dimension: "formula_documented"  # documented | reproducibility | traceability | timing
  target: "column:customers.health_score"
  
  score: 0.75
  confidence: 0.80
  
  evidence:
    - type: "no_formula"
      description: "Derived column with no documented calculation"
    - type: "reverse_engineering_attempt"
      method: "correlation_analysis"
      candidate_formulas:
        - formula: "0.4 * recency_norm + 0.3 * frequency_norm + 0.3 * monetary_norm"
          correlation: 0.87
          interpretation: "RFM-style scoring"
        - formula: "orders_last_90d * avg_order_value / 1000"
          correlation: 0.62
      best_match: "RFM-style"
      confidence: 0.65
    - type: "input_availability"
      likely_inputs: ["last_order_date", "order_count", "total_spend"]
      inputs_present: true
      
  resolution_options:
    - action: "document_formula"
      parameters:
        formula: "0.4 * recency_score + 0.3 * frequency_score + 0.3 * monetary_score"
        components:
          recency_score: "100 - (days_since_last_order / max_days * 100)"
          frequency_score: "order_count_12m / max_orders * 100"
          monetary_score: "total_spend_12m / max_spend * 100"
        normalization: "0-100 scale"
      expected_entropy_reduction: 0.65
      effort: "medium"
    - action: "create_calculated_column"
      parameters:
        replace_with_explicit: true
        formula_in_sql: true
      expected_entropy_reduction: 0.70
      effort: "high"
      
  llm_context:
    description: "Customer health_score has no documented formula. Statistical analysis suggests RFM-based calculation (recency, frequency, monetary) with ~87% correlation."
    query_impact: "Cannot explain score changes or recalculate for projections. Cannot validate against business expectations."
    inferred_formula: "Likely RFM: 0.4*recency + 0.3*frequency + 0.3*monetary"
    inference_confidence: 0.65
    
  human_context:
    severity: "high"
    category: "Calculations"
    message: "Derived metric without formula"
    recommendation: "Document calculation; consider replacing with explicit SQL formula"
```

---

### 5.2 Business Rules

#### Question Asked
What domain logic affects this data?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `rules_documented` | Are business rules explicit? | Rule catalog maintained | Rules in code comments only |
| `rule_coverage` | What % of data has applicable rules? | Rules for all scenarios | Many edge cases unhandled |
| `exception_handling` | How are rule violations handled? | Documented exceptions | Silent failures |
| `rule_versioning` | Are rule changes tracked? | Effective dates recorded | Rules changed without notice |

**Examples:**
- Low entropy: "Intercompany transactions eliminated from consolidated view; rule ID: FIN-001"
- High entropy: Q4 numbers don't match monthly sum, no explanation

#### Explanation
Business rule entropy causes unexplainable discrepancies. When consolidated revenue doesn't match sum of entities, users lose trust. LLMs need business rule context to explain apparent inconsistencies.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "computational"
  dimension: "business_rules"
  sub_dimension: "rules_documented"  # documented | coverage | exceptions | versioning
  target: "table:consolidated_financials"
  
  score: 0.62
  confidence: 0.78
  
  evidence:
    - type: "implicit_rule_detected"
      detection_method: "variance_analysis"
      pattern: "consolidated != sum(entity_level)"
      variance_amount: "3-5% systematic difference"
      likely_explanation: "Intercompany eliminations"
    - type: "temporal_anomaly"
      pattern: "Q4 values include ~2% adjustment not in monthly"
      likely_explanation: "Year-end audit adjustments"
    - type: "rule_gap"
      documented_rules: 0
      inferred_rules: 2
      
  resolution_options:
    - action: "document_business_rules"
      parameters:
        rules:
          - id: "FIN-001"
            name: "Intercompany Elimination"
            description: "Transactions between group entities eliminated in consolidation"
            affected_tables: ["consolidated_financials"]
            materiality: "typically 3-5% of revenue"
          - id: "FIN-002"
            name: "Year-End Adjustments"
            description: "Q4 includes audit adjustments not reflected in monthly"
            affected_periods: ["Q4", "annual"]
            materiality: "typically 1-3% of annual"
      expected_entropy_reduction: 0.50
      effort: "medium"
      
  llm_context:
    description: "Consolidated financials have implicit business rules: intercompany eliminations (3-5% variance from entity sum) and Q4 audit adjustments (~2% quarterly variance)."
    query_impact: "Comparisons of consolidated to entity-level or monthly-to-quarterly will show discrepancies. These are features, not bugs."
    explanation_template: "The difference between consolidated and entity-sum is due to intercompany elimination rules."
    
  human_context:
    severity: "medium"
    category: "Business Logic"
    message: "Undocumented business rules detected"
    recommendation: "Formalize rules in metadata with explanations and materiality"
```

---

### 5.3 Filters

#### Question Asked
Are there implicit data exclusions?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `default_filters_documented` | Are standard exclusions stated? | "Excludes cancelled orders" | Implicit WHERE clauses |
| `filter_scope` | What data is affected? | Clear table/column scope | Unclear application |
| `override_possibility` | Can defaults be bypassed? | Documented override syntax | Filters hard-coded |
| `filter_rationale` | Why does filter exist? | Business reason documented | No explanation |

**Examples:**
- Low entropy: Metric definition includes "WHERE status != 'cancelled' AND is_test = false"
- High entropy: Dashboard shows different totals than raw table, no explanation

#### Explanation
Filter entropy causes number mismatches. When a dashboard shows 10,000 orders but the table has 12,000, users don't trust the data. LLMs need to know about and explain default filters.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "computational"
  dimension: "filters"
  sub_dimension: "default_filters"  # documented | scope | override | rationale
  target: "metric:active_customers"
  
  score: 0.52
  confidence: 0.85
  
  evidence:
    - type: "implicit_filter_detected"
      metric_value: 8934
      raw_count: 12456
      difference: 3522
      likely_filters:
        - filter: "status = 'active'"
          effect: -2100
          confidence: 0.90
        - filter: "is_test_account = false"
          effect: -850
          confidence: 0.85
        - filter: "has_order_last_12m = true"
          effect: -572
          confidence: 0.70
    - type: "documentation_gap"
      metric_definition: "Count of active customers"
      missing_detail: "No WHERE clause specified"
      
  resolution_options:
    - action: "document_filters"
      parameters:
        metric_definition:
          name: "active_customers"
          sql: "SELECT COUNT(*) FROM customers WHERE status = 'active' AND is_test_account = false AND last_order_date > CURRENT_DATE - INTERVAL '12 months'"
          explanation: "Customers with active status, excluding test accounts, with purchase in last 12 months"
      expected_entropy_reduction: 0.45
      effort: "low"
    - action: "create_companion_metrics"
      parameters:
        metrics:
          - name: "active_customers_with_tests"
            includes_test: true
          - name: "all_customers"
            no_filters: true
      expected_entropy_reduction: 0.35
      effort: "medium"
      
  llm_context:
    description: "Active customers metric (8,934) excludes ~3,500 records via implicit filters: inactive status, test accounts, and no recent orders."
    query_impact: "Raw table counts will differ from metric. Explain that 'active customers' has specific criteria."
    filter_summary: "Active status AND not test AND ordered in last 12 months"
    
  human_context:
    severity: "medium"
    category: "Metric Definitions"
    message: "Metric has undocumented filters"
    recommendation: "Add explicit filter definition to metric metadata"
```

---

### 5.4 Aggregations

#### Question Asked
How should values be combined?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `aggregation_rules` | Is rollup method defined? | "SUM for revenue, AVG for rate" | No rules, default to SUM |
| `weighting_documented` | Are weights explicit? | Weight column identified | Unweighted when should be weighted |
| `null_handling_in_agg` | How are nulls treated? | "Exclude nulls from AVG" | Default database behavior |
| `hierarchy_aggregation` | How to roll up dimensions? | Aggregation per level defined | Same rule assumed for all |

**Examples:**
- Low entropy: "Satisfaction: weighted average by response count; unit price: no aggregation (show 'varies')"
- High entropy: "price" column with no guidance—SUM makes no sense, AVG might

#### Explanation
Aggregation entropy produces meaningless numbers. Summing unit prices is nonsensical. Unweighted averages of rates across different-sized segments are misleading. LLMs need aggregation rules to generate correct GROUP BY queries.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "computational"
  dimension: "aggregations"
  sub_dimension: "aggregation_rules"  # rules | weighting | null_handling | hierarchy
  target: "column:products.unit_price"
  
  score: 0.45
  confidence: 0.88
  
  evidence:
    - type: "aggregation_ambiguity"
      column_type: "measure"
      sensible_aggregations:
        - method: "AVG"
          use_case: "Average product price"
          caveats: "Should be weighted by quantity if calculating basket average"
        - method: "MIN/MAX"
          use_case: "Price range"
        - method: "NONE"
          use_case: "Detail-level only"
      invalid_aggregations:
        - method: "SUM"
          reason: "Summing unit prices is meaningless"
    - type: "weighting_consideration"
      weight_column_available: "quantity"
      weighted_avg_recommended: true
      
  resolution_options:
    - action: "define_aggregation_rule"
      parameters:
        column: "unit_price"
        default_aggregation: "AVG"
        weighted_aggregation:
          method: "weighted_avg"
          weight_column: "quantity"
        invalid_aggregations: ["SUM"]
        notes: "For average transaction value, use weighted average by quantity"
      expected_entropy_reduction: 0.40
      effort: "low"
      
  llm_context:
    description: "Unit price should not be summed. Use AVG for simple average, or weighted average (by quantity) for transaction-weighted price."
    query_impact: "SUM(unit_price) queries should be blocked or warned. Default to AVG with caveat about weighting."
    recommended_aggregation: "AVG (or weighted AVG by quantity)"
    blocked_aggregations: ["SUM"]
    
  human_context:
    severity: "medium"
    category: "Aggregation Rules"
    message: "Aggregation method needs definition"
    recommendation: "Add aggregation rules to column metadata"
```

---

## 6. QUERY ENTROPY

Query entropy captures uncertainty in mapping natural language to data operations.

### 6.1 Linguistics

#### Question Asked
Can natural language be mapped to data concepts unambiguously?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `synonym_coverage` | Are alternate terms mapped? | "revenue", "sales", "income" → revenue | No synonyms defined |
| `abbreviation_expansion` | Are short forms documented? | "YTD" → year_to_date | Abbreviations undefined |
| `multilingual_support` | Are translations available? | "Umsatz" → revenue | Single language only |
| `jargon_definitions` | Is domain language documented? | Industry terms defined | Insider language unexplained |

**Examples:**
- Low entropy: Synonym map: {"revenue": ["sales", "income", "turnover"], "COGS": ["cost of goods", "direct costs"]}
- High entropy: User asks about "ARR" but no definition exists

#### Explanation
Linguistic entropy causes query misinterpretation. If "sales" isn't mapped to the `revenue` column, the LLM might look for a `sales` table that doesn't exist. Synonym maps enable flexible, natural querying.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "query"
  dimension: "linguistics"
  sub_dimension: "synonym_coverage"  # synonyms | abbreviations | multilingual | jargon
  target: "concept:revenue"
  
  score: 0.38
  confidence: 0.82
  
  evidence:
    - type: "synonym_analysis"
      canonical_term: "revenue"
      mapped_columns: ["financials.revenue", "sales.total_revenue"]
      known_synonyms: ["sales", "income"]
      missing_synonyms:
        - term: "turnover"
          usage_frequency: "common in UK English"
        - term: "Umsatz"
          usage_frequency: "German equivalent"
    - type: "abbreviation_gap"
      unmapped_abbreviations:
        - abbrev: "Rev"
          likely_meaning: "revenue"
        - abbrev: "TTM Rev"
          likely_meaning: "trailing twelve months revenue"
          
  resolution_options:
    - action: "extend_synonym_map"
      parameters:
        additions:
          revenue: ["turnover", "Rev", "Umsatz", "top line"]
          margin: ["profit margin", "Marge"]
      expected_entropy_reduction: 0.25
      effort: "low"
    - action: "add_abbreviation_dictionary"
      parameters:
        abbreviations:
          TTM: "trailing twelve months"
          YoY: "year over year"
          MoM: "month over month"
      expected_entropy_reduction: 0.20
      effort: "low"
      
  llm_context:
    description: "Revenue concept has partial synonym coverage. 'Sales' and 'income' are mapped; 'turnover' and German 'Umsatz' are not."
    query_impact: "Queries using unmapped terms may fail to find relevant data or produce no results."
    synonym_map:
      revenue: ["sales", "income"]
    unmapped_likely_synonyms: ["turnover", "Umsatz", "top line"]
    
  human_context:
    severity: "low"
    category: "Query Mapping"
    message: "Incomplete synonym coverage"
    recommendation: "Add missing synonyms to term mapping"
```

---

### 6.2 Intent

#### Question Asked
Can user intent be interpreted deterministically?

#### Metrics & Examples

| Metric | Description | Low Entropy | High Entropy |
|--------|-------------|-------------|--------------|
| `temporal_defaults` | Are time scope defaults set? | "Default: last 12 months" | No default, must always specify |
| `scope_defaults` | Are inclusion boundaries clear? | "All regions unless specified" | Unclear what's included |
| `aggregation_defaults` | Is default detail level defined? | "Summary unless detail requested" | Unclear granularity |
| `output_conventions` | Are presentation defaults set? | "Currency with 2 decimals" | No formatting defaults |

**Examples:**
- Low entropy: "Show me revenue" → uses default time (YTD), currency (EUR), granularity (monthly)
- High entropy: "Show me revenue" → requires 5 clarifying questions

#### Explanation
Intent entropy determines how natural the query experience feels. High intent entropy means users must over-specify every query. Low entropy means reasonable defaults allow terse, natural questions.

#### Entropy Object Structure

```yaml
entropy_object:
  layer: "query"
  dimension: "intent"
  sub_dimension: "temporal_defaults"  # temporal | scope | aggregation | output
  target: "domain:financial_queries"
  
  score: 0.55
  confidence: 0.75
  
  evidence:
    - type: "missing_defaults"
      missing:
        - category: "time_period"
          impact: "Every financial query requires date specification"
          frequency: "affects ~80% of queries"
        - category: "currency"
          impact: "Multi-currency environment needs default"
          frequency: "affects ~30% of queries"
    - type: "clarification_analysis"
      avg_clarifications_per_query: 2.3
      most_common_clarifications:
        - "Which time period?"
        - "Which currency?"
        - "Include or exclude forecasts?"
        
  resolution_options:
    - action: "define_query_defaults"
      parameters:
        defaults:
          time_period: "trailing_twelve_months"
          currency: "report_in_eur"
          include_forecast: false
          granularity: "monthly"
        override_phrases:
          time: ["YTD", "this quarter", "last year", "all time"]
          currency: ["in CHF", "in local currency"]
      expected_entropy_reduction: 0.40
      effort: "medium"
    - action: "create_intent_templates"
      parameters:
        templates:
          - pattern: "show me {metric}"
            defaults: {period: "TTM", granularity: "monthly"}
          - pattern: "{metric} by {dimension}"
            defaults: {period: "TTM", aggregation: "sum"}
      expected_entropy_reduction: 0.35
      effort: "medium"
      
  llm_context:
    description: "Financial queries lack defaults for time period and currency. Users must specify these for every query."
    query_impact: "Ambiguous queries require clarification, reducing conversational flow."
    suggested_defaults:
      time_period: "trailing twelve months"
      currency: "EUR"
      note: "Defaults should be confirmed with business stakeholders"
    clarification_prompt: "For what time period? (Default: last 12 months)"
    
  human_context:
    severity: "medium"
    category: "Query Experience"
    message: "No query defaults configured"
    recommendation: "Define sensible defaults for common query parameters"
```

---

## Implementation Guide for Claude Code

This section provides instructions for implementing the entropy management framework. These instructions are designed to be technology-agnostic—the implementing agent should discover the existing codebase and adapt accordingly.

### Phase 1: Discovery

Before implementing, explore and map the existing system:

```markdown
## Discovery Tasks

1. **Explore project structure**
   - Find data ingestion code (look for: dlt, airbyte, custom loaders)
   - Find transformation layer (look for: dbt, sqlmesh, custom SQL)
   - Find metadata storage (look for: yaml configs, database tables, JSON)
   - Find existing quality checks (look for: great_expectations, soda, custom)

2. **Map existing patterns**
   - How is data currently profiled?
   - Where is metadata currently stored?
   - What configuration format is used? (YAML, JSON, TOML)
   - Are there existing quality scores or metrics?

3. **Identify integration points**
   - Where should entropy detection run? (ingestion, transformation, query time)
   - Where should entropy objects be stored?
   - How do query agents currently access metadata?

4. **Document technology stack**
   - Database engine (DuckDB, PostgreSQL, etc.)
   - Primary language (Python, etc.)
   - Configuration management approach
   - Existing libraries in use
```

### Phase 2: Configuration Schema

Define the YAML configuration that influences entropy extraction:

```yaml
# entropy_config.yaml
# This configuration controls how entropy is detected and scored

version: "1.0"

# Global settings
global:
  # Default scoring thresholds
  scoring:
    low_entropy_threshold: 0.3    # Below this is "good"
    high_entropy_threshold: 0.7   # Above this needs attention
    
  # Which dimensions to evaluate
  enabled_dimensions:
    source:
      physical: true
      provenance: true
      access_rights: false  # Disable if not relevant
      lifecycle: true
    structural:
      schema: true
      types: true
      relations: true
    semantic:
      business_meaning: true
      units: true
      temporal: true
      categorical: true
    value:
      categories: true
      null_semantics: true
      outliers: true
      ranges: true
      patterns: true
    computational:
      derived_values: true
      business_rules: true
      filters: true
      aggregations: true
    query:
      linguistics: true
      intent: true

# Source-specific configuration
sources:
  - name: "crm_export"
    path: "data/crm/*.csv"
    config:
      physical:
        expected_encoding: "UTF-8"
        expected_delimiter: ","
        locale: "de_CH"
      provenance:
        source_system: "Salesforce"
        trust_level: "medium"  # manual entry
        
  - name: "erp_extract"
    path: "data/erp/*.parquet"
    config:
      provenance:
        source_system: "SAP"
        trust_level: "high"  # system-generated
        
# Table-specific configuration
tables:
  - name: "customers"
    config:
      structural:
        naming_convention: "snake_case"
        expected_columns:
          - name: "customer_id"
            type: "INTEGER"
            nullable: false
          - name: "status"
            type: "VARCHAR"
            expected_values: ["active", "inactive", "pending"]
      semantic:
        business_definitions:
          customer_id: "Unique identifier for customer, from CRM system"
          status: "Current customer lifecycle status"
          
  - name: "financials"
    config:
      semantic:
        units:
          revenue: {unit: "CHF", scale: 1}
          margin: {unit: "percentage", range: [-100, 100]}
        temporal:
          revenue: {accumulation: "ytd_cumulative", reset: "calendar_year"}
      computational:
        derived_columns:
          margin:
            formula: "(revenue - costs) / revenue"
            inputs: ["revenue", "costs"]

# Column pattern detection configuration
pattern_detection:
  # Date patterns to recognize
  date_patterns:
    - format: "YYYY-MM-DD"
      regex: "^\\d{4}-\\d{2}-\\d{2}$"
    - format: "DD.MM.YYYY"
      regex: "^\\d{2}\\.\\d{2}\\.\\d{4}$"
      locale: "de_CH"
      
  # Numeric patterns
  numeric_patterns:
    - name: "german_decimal"
      decimal_separator: ","
      thousands_separator: "."
      
  # Categorical detection
  categorical:
    max_unique_ratio: 0.05  # Column is categorical if unique/total < 5%
    min_occurrences: 10     # Minimum occurrences to be considered a valid category

# Synonym and linguistic mapping
linguistics:
  synonyms:
    revenue: ["sales", "income", "turnover", "Umsatz"]
    customer: ["client", "account", "Kunde"]
    margin: ["profit margin", "Marge"]
    
  abbreviations:
    YTD: "year to date"
    TTM: "trailing twelve months"
    MoM: "month over month"
    YoY: "year over year"
    COGS: "cost of goods sold"
    
# Query defaults
query_defaults:
  financial_metrics:
    time_period: "trailing_twelve_months"
    currency_display: "CHF"
    granularity: "monthly"
    
  override_patterns:
    time_period:
      - pattern: "YTD"
        value: "year_to_date"
      - pattern: "this quarter"
        value: "current_quarter"
      - pattern: "last year"
        value: "previous_calendar_year"

# Resolution action templates
resolution_templates:
  add_column_definition:
    template: |
      columns:
        {column_name}:
          description: "{description}"
          business_term: "{term}"
          
  add_unit_annotation:
    template: |
      semantic:
        units:
          {column_name}: {unit: "{unit}", scale: {scale}}
```

### Phase 3: Entropy Detection Implementation

```markdown
## Implementation Structure

Create the following module structure (adapt to existing patterns):

```
entropy/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── loader.py           # Load and validate YAML config
│   └── schema.py           # Pydantic models for config validation
├── detectors/
│   ├── __init__.py
│   ├── base.py             # Base detector interface
│   ├── source/
│   │   ├── physical.py     # Encoding, format detection
│   │   ├── provenance.py   # Lineage, trust scoring
│   │   └── lifecycle.py    # Freshness, versioning
│   ├── structural/
│   │   ├── schema.py       # Naming, organization analysis
│   │   ├── types.py        # Type fidelity checking
│   │   └── relations.py    # Cardinality, join path analysis
│   ├── semantic/
│   │   ├── business.py     # Definition coverage
│   │   ├── units.py        # Unit detection (integrate Pint)
│   │   ├── temporal.py     # Accumulation, point-in-time
│   │   └── categorical.py  # Hierarchy detection
│   ├── value/
│   │   ├── categories.py   # Variant detection
│   │   ├── nulls.py        # Null semantics
│   │   ├── outliers.py     # Anomaly detection
│   │   ├── ranges.py       # Boundary analysis
│   │   └── patterns.py     # Format detection
│   ├── computational/
│   │   ├── derived.py      # Formula detection
│   │   ├── rules.py        # Business rule inference
│   │   ├── filters.py      # Implicit filter detection
│   │   └── aggregations.py # Aggregation rule inference
│   └── query/
│       ├── linguistics.py  # Synonym coverage
│       └── intent.py       # Default coverage
├── objects/
│   ├── __init__.py
│   ├── entropy_object.py   # Core entropy object class
│   └── serialization.py    # YAML/JSON serialization
├── scoring/
│   ├── __init__.py
│   ├── calculator.py       # Score calculation
│   └── aggregator.py       # Roll-up scores
├── storage/
│   ├── __init__.py
│   ├── base.py             # Storage interface
│   └── duckdb.py           # DuckDB implementation (if applicable)
└── api/
    ├── __init__.py
    ├── query_context.py    # Context generation for query agents
    └── resolution.py       # Resolution action execution
```

## Base Detector Interface

```python
# entropy/detectors/base.py

from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class Evidence:
    """Single piece of evidence for entropy scoring."""
    type: str
    detail: str
    samples: Optional[List] = None
    confidence: float = 1.0
    metadata: Optional[dict] = None

@dataclass 
class ResolutionOption:
    """Possible action to reduce entropy."""
    action: str
    parameters: dict
    expected_entropy_reduction: float
    effort: str  # low, medium, high
    breaking_change: bool = False

@dataclass
class EntropyObject:
    """Core entropy measurement object."""
    layer: str
    dimension: str
    sub_dimension: str
    target: str
    
    score: float  # 0-1
    confidence: float  # 0-1
    
    evidence: List[Evidence]
    resolution_options: List[ResolutionOption]
    
    llm_context: dict
    human_context: dict


class BaseDetector(ABC):
    """Base class for entropy detectors."""
    
    layer: str
    dimension: str
    sub_dimension: str
    
    def __init__(self, config: dict):
        self.config = config
    
    @abstractmethod
    def detect(self, target: str, data: any, metadata: dict) -> EntropyObject:
        """
        Detect entropy for the given target.
        
        Args:
            target: Identifier for what's being analyzed (table, column, etc.)
            data: The actual data to analyze
            metadata: Existing metadata about the target
            
        Returns:
            EntropyObject with score, evidence, and resolution options
        """
        pass
    
    @abstractmethod
    def can_detect(self, target: str, data: any, metadata: dict) -> bool:
        """Check if this detector is applicable to the target."""
        pass
    
    def calculate_score(self, evidence: List[Evidence]) -> float:
        """Calculate entropy score from evidence. Override for custom logic."""
        if not evidence:
            return 0.0
        
        # Default: average of evidence-based scores
        # Subclasses should implement domain-specific scoring
        raise NotImplementedError("Subclass must implement scoring logic")
    
    def generate_llm_context(self, evidence: List[Evidence], score: float) -> dict:
        """Generate context for LLM consumption."""
        raise NotImplementedError("Subclass must implement LLM context generation")
    
    def generate_human_context(self, evidence: List[Evidence], score: float) -> dict:
        """Generate context for human consumption."""
        raise NotImplementedError("Subclass must implement human context generation")
```

## Example Detector Implementation

```python
# entropy/detectors/semantic/units.py

from ..base import BaseDetector, EntropyObject, Evidence, ResolutionOption
from typing import List, Optional
import re

class UnitDetector(BaseDetector):
    """Detect entropy in unit/measurement specification."""
    
    layer = "semantic"
    dimension = "units"
    sub_dimension = "unit_declared"
    
    # Common unit patterns
    CURRENCY_PATTERNS = [
        (r'_eur$|_euro$', 'EUR'),
        (r'_chf$', 'CHF'),
        (r'_usd$|_dollar', 'USD'),
    ]
    
    UNIT_PATTERNS = [
        (r'_pct$|_percent', 'percentage'),
        (r'_kg$|_kilogram', 'kg'),
        (r'_count$|_cnt$', 'count'),
    ]
    
    def can_detect(self, target: str, data: any, metadata: dict) -> bool:
        """Applicable to numeric columns."""
        # Check if column is numeric - adapt to your data representation
        return metadata.get('dtype', '').startswith(('int', 'float', 'decimal'))
    
    def detect(self, target: str, data: any, metadata: dict) -> EntropyObject:
        evidence = []
        
        # Check for explicit unit declaration
        declared_unit = metadata.get('unit')
        if declared_unit:
            evidence.append(Evidence(
                type="unit_declared",
                detail=f"Unit explicitly declared: {declared_unit}",
                confidence=1.0
            ))
            score = 0.1  # Very low entropy if declared
        else:
            evidence.append(Evidence(
                type="no_unit_declaration",
                detail="Column has no unit metadata",
                confidence=1.0
            ))
            
            # Try to infer from column name
            column_name = target.split('.')[-1].lower()
            inferred_unit = self._infer_unit_from_name(column_name)
            
            if inferred_unit:
                evidence.append(Evidence(
                    type="unit_inferred_from_name",
                    detail=f"Unit possibly '{inferred_unit}' based on column name",
                    confidence=0.7
                ))
                score = 0.4  # Medium entropy - inferred but not declared
            else:
                # Try to infer from values
                value_inference = self._infer_unit_from_values(data, metadata)
                if value_inference:
                    evidence.append(Evidence(
                        type="unit_inferred_from_values",
                        detail=value_inference['detail'],
                        confidence=value_inference['confidence'],
                        samples=value_inference.get('samples')
                    ))
                    score = 0.6  # Higher entropy - weak inference
                else:
                    score = 0.8  # High entropy - no inference possible
        
        # Generate resolution options
        resolution_options = self._generate_resolutions(target, evidence, metadata)
        
        return EntropyObject(
            layer=self.layer,
            dimension=self.dimension,
            sub_dimension=self.sub_dimension,
            target=target,
            score=score,
            confidence=self._calculate_confidence(evidence),
            evidence=evidence,
            resolution_options=resolution_options,
            llm_context=self.generate_llm_context(evidence, score),
            human_context=self.generate_human_context(evidence, score)
        )
    
    def _infer_unit_from_name(self, name: str) -> Optional[str]:
        """Infer unit from column naming patterns."""
        for pattern, unit in self.CURRENCY_PATTERNS + self.UNIT_PATTERNS:
            if re.search(pattern, name):
                return unit
        return None
    
    def _infer_unit_from_values(self, data, metadata) -> Optional[dict]:
        """Infer unit from value patterns."""
        # Adapt this to your data representation
        # Example: check value ranges, decimal precision, etc.
        
        if metadata.get('decimal_places') == 2:
            if metadata.get('max', 0) < 10000:
                return {
                    'detail': 'Values suggest currency (2 decimal places, typical range)',
                    'confidence': 0.5,
                    'samples': metadata.get('sample_values', [])
                }
        return None
    
    def _generate_resolutions(self, target, evidence, metadata) -> List[ResolutionOption]:
        """Generate resolution options based on evidence."""
        options = []
        
        # Always offer explicit declaration
        options.append(ResolutionOption(
            action="declare_unit",
            parameters={
                "target": target,
                "unit": "<specify_unit>",
                "scale": 1
            },
            expected_entropy_reduction=0.7,
            effort="low"
        ))
        
        # If we inferred a unit, offer to confirm it
        for ev in evidence:
            if 'inferred' in ev.type:
                inferred = ev.detail.split("'")[1] if "'" in ev.detail else None
                if inferred:
                    options.append(ResolutionOption(
                        action="confirm_inferred_unit",
                        parameters={
                            "target": target,
                            "unit": inferred
                        },
                        expected_entropy_reduction=0.6,
                        effort="low"
                    ))
        
        return options
    
    def _calculate_confidence(self, evidence: List[Evidence]) -> float:
        """Calculate overall confidence from evidence."""
        if not evidence:
            return 0.5
        return sum(e.confidence for e in evidence) / len(evidence)
    
    def generate_llm_context(self, evidence: List[Evidence], score: float) -> dict:
        """Generate context for LLM query agent."""
        context = {
            "description": "",
            "query_impact": "",
            "assumptions_if_unresolved": []
        }
        
        if score < 0.3:
            context["description"] = "Unit is clearly defined."
            context["query_impact"] = "None - unit operations are safe."
        elif score < 0.6:
            inferred = next((e for e in evidence if 'inferred' in e.type), None)
            if inferred:
                context["description"] = f"Unit not declared but inferred: {inferred.detail}"
                context["query_impact"] = "Use inferred unit with caution in calculations."
                context["assumptions_if_unresolved"] = [{
                    "assumption": inferred.detail,
                    "confidence": inferred.confidence
                }]
        else:
            context["description"] = "Unit is not declared and cannot be reliably inferred."
            context["query_impact"] = "Currency conversions and unit comparisons are unreliable."
            context["assumptions_if_unresolved"] = [{
                "assumption": "Unknown unit - avoid calculations requiring unit knowledge",
                "confidence": 0.0
            }]
        
        return context
    
    def generate_human_context(self, evidence: List[Evidence], score: float) -> dict:
        """Generate context for human UI."""
        severity = "low" if score < 0.3 else "medium" if score < 0.6 else "high"
        
        return {
            "severity": severity,
            "category": "Units & Measurement",
            "message": self._summarize_evidence(evidence),
            "recommendation": "Add unit declaration to column metadata"
        }
    
    def _summarize_evidence(self, evidence: List[Evidence]) -> str:
        """Create human-readable summary."""
        for e in evidence:
            if e.type == "no_unit_declaration":
                return "Unit not specified for numeric column"
            if e.type == "unit_declared":
                return "Unit properly declared"
        return "Unit status unclear"
```
```

### Phase 4: Query Agent Integration

```markdown
## Entropy Context for Query Agents

The query agent needs entropy information at query time to:
1. Decide if a query can be answered deterministically
2. Add appropriate caveats to responses
3. Ask clarifying questions when needed
4. Surface assumptions being made

### Context Generation

```python
# entropy/api/query_context.py

from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class QueryEntropyContext:
    """Entropy context for a specific query."""
    
    # Overall query feasibility
    deterministic: bool
    confidence: float
    
    # Relevant entropy objects
    entropy_objects: List[dict]
    
    # Summarized impacts
    assumptions_required: List[dict]
    clarifications_suggested: List[dict]
    caveats_for_response: List[str]
    
    # Resolution hints
    blocking_entropy: List[dict]  # Must be resolved before query
    degrading_entropy: List[dict]  # Reduces confidence but answerable


class QueryContextGenerator:
    """Generate entropy context for query agents."""
    
    def __init__(self, entropy_store):
        self.entropy_store = entropy_store
    
    def get_context_for_query(
        self, 
        query: str, 
        referenced_tables: List[str],
        referenced_columns: List[str]
    ) -> QueryEntropyContext:
        """
        Generate entropy context for a natural language query.
        
        Args:
            query: The natural language query
            referenced_tables: Tables the query will access
            referenced_columns: Columns the query will use
            
        Returns:
            QueryEntropyContext with all relevant entropy information
        """
        # Gather all relevant entropy objects
        entropy_objects = []
        
        for table in referenced_tables:
            entropy_objects.extend(
                self.entropy_store.get_for_target(f"table:{table}")
            )
            
        for column in referenced_columns:
            entropy_objects.extend(
                self.entropy_store.get_for_target(f"column:{column}")
            )
        
        # Analyze query-specific entropy
        query_entropy = self._analyze_query_entropy(query)
        entropy_objects.extend(query_entropy)
        
        # Categorize by impact
        blocking = [e for e in entropy_objects if e['score'] > 0.8]
        degrading = [e for e in entropy_objects if 0.5 < e['score'] <= 0.8]
        
        # Generate response components
        assumptions = self._extract_assumptions(entropy_objects)
        clarifications = self._suggest_clarifications(entropy_objects, query)
        caveats = self._generate_caveats(entropy_objects)
        
        # Calculate overall confidence
        deterministic = len(blocking) == 0
        confidence = self._calculate_query_confidence(entropy_objects)
        
        return QueryEntropyContext(
            deterministic=deterministic,
            confidence=confidence,
            entropy_objects=[e for e in entropy_objects],
            assumptions_required=assumptions,
            clarifications_suggested=clarifications,
            caveats_for_response=caveats,
            blocking_entropy=blocking,
            degrading_entropy=degrading
        )
    
    def format_for_llm(self, context: QueryEntropyContext) -> str:
        """Format entropy context for injection into LLM prompt."""
        
        sections = []
        
        # Confidence summary
        if context.deterministic:
            sections.append(
                f"Query confidence: {context.confidence:.0%} - can answer deterministically"
            )
        else:
            sections.append(
                f"Query confidence: {context.confidence:.0%} - has blocking uncertainties"
            )
        
        # Blocking issues
        if context.blocking_entropy:
            sections.append("\n**Cannot answer reliably due to:**")
            for e in context.blocking_entropy:
                sections.append(f"- {e['llm_context']['description']}")
        
        # Required assumptions
        if context.assumptions_required:
            sections.append("\n**Assumptions being made:**")
            for a in context.assumptions_required:
                sections.append(
                    f"- {a['assumption']} (confidence: {a['confidence']:.0%})"
                )
        
        # Suggested clarifications
        if context.clarifications_suggested:
            sections.append("\n**Consider asking user to clarify:**")
            for c in context.clarifications_suggested:
                sections.append(f"- {c['question']}")
        
        # Caveats for response
        if context.caveats_for_response:
            sections.append("\n**Include in response:**")
            for c in context.caveats_for_response:
                sections.append(f"- {c}")
        
        return "\n".join(sections)
    
    def _analyze_query_entropy(self, query: str) -> List[dict]:
        """Analyze entropy specific to the query itself."""
        # Check for temporal ambiguity
        # Check for scope ambiguity
        # Check for aggregation ambiguity
        # etc.
        pass
    
    def _extract_assumptions(self, entropy_objects: List[dict]) -> List[dict]:
        """Extract assumptions from entropy objects."""
        assumptions = []
        for e in entropy_objects:
            if 'assumptions_if_unresolved' in e.get('llm_context', {}):
                assumptions.extend(e['llm_context']['assumptions_if_unresolved'])
        return assumptions
    
    def _suggest_clarifications(
        self, 
        entropy_objects: List[dict], 
        query: str
    ) -> List[dict]:
        """Suggest clarifying questions based on entropy."""
        clarifications = []
        
        for e in entropy_objects:
            if e['score'] > 0.6:  # High entropy
                # Generate clarifying question based on dimension
                question = self._generate_clarification(e, query)
                if question:
                    clarifications.append({
                        'question': question,
                        'entropy_dimension': f"{e['layer']}.{e['dimension']}",
                        'impact': e['llm_context'].get('query_impact', '')
                    })
        
        return clarifications
    
    def _generate_caveats(self, entropy_objects: List[dict]) -> List[str]:
        """Generate caveats to include in response."""
        caveats = []
        
        for e in entropy_objects:
            if 0.4 < e['score'] <= 0.7:  # Medium entropy
                caveat = e['llm_context'].get('assumption_disclosure')
                if caveat:
                    caveats.append(caveat)
        
        return list(set(caveats))  # Deduplicate
    
    def _calculate_query_confidence(self, entropy_objects: List[dict]) -> float:
        """Calculate overall query confidence."""
        if not entropy_objects:
            return 1.0
        
        # Weighted average, with higher-entropy objects having more impact
        total_weight = 0
        weighted_confidence = 0
        
        for e in entropy_objects:
            weight = e['score']  # Higher entropy = more weight
            confidence = 1 - e['score']
            weighted_confidence += weight * confidence
            total_weight += weight
        
        if total_weight == 0:
            return 1.0
            
        return weighted_confidence / total_weight
    
    def _generate_clarification(self, entropy_obj: dict, query: str) -> Optional[str]:
        """Generate clarifying question for specific entropy."""
        templates = {
            ('semantic', 'units'): "What currency/unit should be used for {target}?",
            ('semantic', 'temporal'): "For what time period? (e.g., YTD, last 12 months)",
            ('structural', 'relations'): "Which relationship should be used: {options}?",
            ('query', 'intent'): "Could you clarify: {ambiguity}?",
        }
        
        key = (entropy_obj['layer'], entropy_obj['dimension'])
        template = templates.get(key)
        
        if template:
            return template.format(
                target=entropy_obj['target'],
                options=entropy_obj.get('llm_context', {}).get('options', ''),
                ambiguity=entropy_obj.get('llm_context', {}).get('ambiguity', '')
            )
        
        return None
```

### Entropy Score Calculation

```python
# entropy/scoring/calculator.py

from typing import List, Dict
from dataclasses import dataclass

@dataclass
class ScoreWeights:
    """Configurable weights for entropy scoring."""
    
    # Evidence type weights
    evidence_weights: Dict[str, float] = None
    
    # Dimension importance weights
    dimension_weights: Dict[str, float] = None
    
    def __post_init__(self):
        self.evidence_weights = self.evidence_weights or {
            'missing': 1.0,          # Something is missing
            'inferred': 0.6,         # We guessed
            'inconsistent': 0.8,     # Data doesn't agree
            'ambiguous': 0.7,        # Multiple interpretations
            'declared': 0.0,         # Explicitly defined
            'validated': -0.1,       # Verified correct
        }
        
        self.dimension_weights = self.dimension_weights or {
            # These affect "how bad" entropy in each dimension is
            'source.physical': 1.0,
            'source.provenance': 0.8,
            'source.access_rights': 0.6,
            'source.lifecycle': 0.7,
            'structural.schema': 0.9,
            'structural.types': 0.95,
            'structural.relations': 0.85,
            'semantic.business_meaning': 1.0,
            'semantic.units': 0.95,
            'semantic.temporal': 0.9,
            'semantic.categorical': 0.7,
            'value.categories': 0.6,
            'value.null_semantics': 0.7,
            'value.outliers': 0.5,
            'value.ranges': 0.5,
            'value.patterns': 0.6,
            'computational.derived_values': 0.9,
            'computational.business_rules': 0.8,
            'computational.filters': 0.85,
            'computational.aggregations': 0.8,
            'query.linguistics': 0.5,
            'query.intent': 0.6,
        }


class EntropyScoreCalculator:
    """Calculate entropy scores from evidence."""
    
    def __init__(self, weights: ScoreWeights = None):
        self.weights = weights or ScoreWeights()
    
    def calculate_from_evidence(
        self, 
        evidence: List[Dict],
        dimension: str
    ) -> float:
        """
        Calculate entropy score from list of evidence items.
        
        Score is 0-1 where:
        - 0 = fully deterministic, no uncertainty
        - 1 = maximum uncertainty, cannot be used reliably
        """
        if not evidence:
            return 0.5  # Unknown = medium entropy
        
        # Base score from evidence types
        evidence_scores = []
        for e in evidence:
            base = self._score_evidence_type(e['type'])
            confidence = e.get('confidence', 1.0)
            evidence_scores.append(base * confidence)
        
        # Combine evidence scores (not simple average - presence of high entropy matters)
        if not evidence_scores:
            raw_score = 0.5
        else:
            # Use max + weighted average to ensure high entropy items dominate
            max_score = max(evidence_scores)
            avg_score = sum(evidence_scores) / len(evidence_scores)
            raw_score = 0.6 * max_score + 0.4 * avg_score
        
        # Apply dimension weight
        dimension_weight = self.weights.dimension_weights.get(dimension, 1.0)
        
        # Final score clamped to 0-1
        return max(0.0, min(1.0, raw_score * dimension_weight))
    
    def _score_evidence_type(self, evidence_type: str) -> float:
        """Get base score for evidence type."""
        # Match partial evidence types
        for key, weight in self.weights.evidence_weights.items():
            if key in evidence_type.lower():
                return weight
        
        return 0.5  # Default for unknown types
    
    def calculate_aggregate(
        self, 
        entropy_objects: List[Dict],
        aggregation_level: str = 'table'
    ) -> Dict:
        """
        Calculate aggregate entropy scores.
        
        Args:
            entropy_objects: List of entropy objects to aggregate
            aggregation_level: 'column', 'table', 'dataset', 'dimension'
            
        Returns:
            Aggregated scores by the specified level
        """
        if aggregation_level == 'dimension':
            return self._aggregate_by_dimension(entropy_objects)
        elif aggregation_level == 'table':
            return self._aggregate_by_table(entropy_objects)
        elif aggregation_level == 'dataset':
            return self._aggregate_overall(entropy_objects)
        else:
            raise ValueError(f"Unknown aggregation level: {aggregation_level}")
    
    def _aggregate_by_dimension(self, objects: List[Dict]) -> Dict:
        """Aggregate scores by entropy dimension."""
        by_dimension = {}
        
        for obj in objects:
            dim_key = f"{obj['layer']}.{obj['dimension']}"
            if dim_key not in by_dimension:
                by_dimension[dim_key] = []
            by_dimension[dim_key].append(obj['score'])
        
        return {
            dim: {
                'mean': sum(scores) / len(scores),
                'max': max(scores),
                'count': len(scores)
            }
            for dim, scores in by_dimension.items()
        }
    
    def _aggregate_by_table(self, objects: List[Dict]) -> Dict:
        """Aggregate scores by table."""
        by_table = {}
        
        for obj in objects:
            # Extract table from target (e.g., "column:sales.revenue" -> "sales")
            target = obj['target']
            if ':' in target:
                target = target.split(':')[1]
            table = target.split('.')[0]
            
            if table not in by_table:
                by_table[table] = []
            by_table[table].append(obj['score'])
        
        return {
            table: {
                'mean': sum(scores) / len(scores),
                'max': max(scores),
                'count': len(scores),
                'readiness': 1 - (sum(scores) / len(scores))  # Inverse of avg entropy
            }
            for table, scores in by_table.items()
        }
    
    def _aggregate_overall(self, objects: List[Dict]) -> Dict:
        """Calculate overall dataset entropy."""
        if not objects:
            return {'score': 0.5, 'readiness': 0.5, 'count': 0}
        
        scores = [obj['score'] for obj in objects]
        mean_score = sum(scores) / len(scores)
        
        return {
            'score': mean_score,
            'readiness': 1 - mean_score,
            'max_entropy': max(scores),
            'min_entropy': min(scores),
            'count': len(scores),
            'high_entropy_count': sum(1 for s in scores if s > 0.7),
            'medium_entropy_count': sum(1 for s in scores if 0.3 < s <= 0.7),
            'low_entropy_count': sum(1 for s in scores if s <= 0.3)
        }
```
```

### Phase 5: Resolution Hint Generation

```markdown
## Resolution Hints

Resolution hints guide users toward reducing entropy. They should be:
- Specific and actionable
- Prioritized by impact
- Include effort estimates
- Link to configuration changes

```python
# entropy/api/resolution.py

from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class ResolutionHint:
    """Actionable hint for reducing entropy."""
    
    # What to do
    action: str
    description: str
    
    # Impact
    target_entropy_object: str
    current_score: float
    expected_score_after: float
    entropy_reduction: float
    
    # Effort
    effort: str  # low, medium, high
    breaking_change: bool
    requires_human_input: bool
    
    # How to do it
    config_change: Optional[Dict]  # YAML snippet to add/modify
    sql_change: Optional[str]      # SQL to run
    manual_steps: Optional[List[str]]
    
    # Priority
    priority_score: float  # Higher = do first


class ResolutionHintGenerator:
    """Generate prioritized resolution hints."""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
    
    def generate_hints(
        self, 
        entropy_objects: List[Dict],
        max_hints: int = 10,
        effort_filter: Optional[str] = None  # 'low', 'medium', 'high'
    ) -> List[ResolutionHint]:
        """
        Generate prioritized resolution hints.
        
        Args:
            entropy_objects: Entropy objects to generate hints for
            max_hints: Maximum number of hints to return
            effort_filter: Only return hints of this effort level
            
        Returns:
            List of ResolutionHint, sorted by priority
        """
        hints = []
        
        for obj in entropy_objects:
            if obj['score'] < 0.3:  # Skip low-entropy objects
                continue
                
            for resolution in obj.get('resolution_options', []):
                hint = self._create_hint(obj, resolution)
                
                if effort_filter and hint.effort != effort_filter:
                    continue
                    
                hints.append(hint)
        
        # Sort by priority (highest first)
        hints.sort(key=lambda h: h.priority_score, reverse=True)
        
        return hints[:max_hints]
    
    def _create_hint(self, entropy_obj: Dict, resolution: Dict) -> ResolutionHint:
        """Create a resolution hint from entropy object and resolution option."""
        
        # Calculate priority score
        # Higher priority for: high entropy, low effort, high impact
        entropy_reduction = resolution.get('expected_entropy_reduction', 0.5)
        effort_score = {'low': 1.0, 'medium': 0.6, 'high': 0.3}.get(
            resolution.get('effort', 'medium'), 0.5
        )
        current_score = entropy_obj['score']
        
        priority = current_score * entropy_reduction * effort_score
        
        # Generate config change
        config_change = self._generate_config_change(
            entropy_obj, 
            resolution
        )
        
        return ResolutionHint(
            action=resolution['action'],
            description=self._describe_resolution(resolution),
            target_entropy_object=entropy_obj['target'],
            current_score=current_score,
            expected_score_after=current_score - entropy_reduction,
            entropy_reduction=entropy_reduction,
            effort=resolution.get('effort', 'medium'),
            breaking_change=resolution.get('breaking_change', False),
            requires_human_input=self._requires_human_input(resolution),
            config_change=config_change,
            sql_change=resolution.get('sql'),
            manual_steps=resolution.get('manual_steps'),
            priority_score=priority
        )
    
    def _generate_config_change(
        self, 
        entropy_obj: Dict, 
        resolution: Dict
    ) -> Optional[Dict]:
        """Generate YAML config snippet for resolution."""
        
        action = resolution['action']
        params = resolution.get('parameters', {})
        target = entropy_obj['target']
        
        # Parse target
        target_type, target_path = target.split(':', 1) if ':' in target else ('unknown', target)
        
        if action == 'declare_unit':
            return {
                'path': f'tables.{target_path.split(".")[0]}.config.semantic.units',
                'change': {
                    target_path.split('.')[-1]: {
                        'unit': params.get('unit', '<specify>'),
                        'scale': params.get('scale', 1)
                    }
                }
            }
        
        elif action == 'add_definition':
            return {
                'path': f'tables.{target_path.split(".")[0]}.config.semantic.business_definitions',
                'change': {
                    target_path.split('.')[-1]: params.get('definition', '<add definition>')
                }
            }
        
        elif action == 'define_temporal_semantics':
            return {
                'path': f'tables.{target_path.split(".")[0]}.config.semantic.temporal',
                'change': {
                    target_path.split('.')[-1]: {
                        'accumulation': params.get('accumulation_type', '<specify>'),
                        'reset': params.get('reset_period', '<specify>')
                    }
                }
            }
        
        elif action == 'create_normalization_mapping':
            return {
                'path': f'tables.{target_path.split(".")[0]}.config.value.normalizations',
                'change': {
                    target_path.split('.')[-1]: {
                        'mappings': params.get('mappings', [])
                    }
                }
            }
        
        return None
    
    def _describe_resolution(self, resolution: Dict) -> str:
        """Generate human-readable description."""
        templates = {
            'declare_unit': "Add unit declaration ({unit}) to column metadata",
            'add_definition': "Add business definition to glossary",
            'define_temporal_semantics': "Document temporal accumulation behavior",
            'create_normalization_mapping': "Create value normalization mappings",
            'document_formula': "Document calculation formula",
            'declare_canonical_relationship': "Designate primary join path",
            'add_column_alias': "Add descriptive alias to column",
        }
        
        action = resolution['action']
        template = templates.get(action, f"Apply resolution: {action}")
        
        return template.format(**resolution.get('parameters', {}))
    
    def _requires_human_input(self, resolution: Dict) -> bool:
        """Check if resolution requires human decision."""
        # Actions that always need human input
        human_required = [
            'add_definition',
            'define_temporal_semantics', 
            'document_formula',
            'document_business_rules'
        ]
        
        return resolution['action'] in human_required
    
    def format_hints_for_human(self, hints: List[ResolutionHint]) -> str:
        """Format hints for human-readable output."""
        
        if not hints:
            return "No resolution hints - data is well-configured!"
        
        lines = ["## Recommended Actions to Reduce Entropy\n"]
        
        for i, hint in enumerate(hints, 1):
            lines.append(f"### {i}. {hint.description}")
            lines.append(f"- **Target**: {hint.target_entropy_object}")
            lines.append(f"- **Current entropy**: {hint.current_score:.2f} → {hint.expected_score_after:.2f}")
            lines.append(f"- **Effort**: {hint.effort}")
            
            if hint.breaking_change:
                lines.append("- ⚠️ **Breaking change**")
            
            if hint.config_change:
                lines.append("\n**Config change:**")
                lines.append(f"```yaml\n# Add to {hint.config_change['path']}")
                lines.append(f"{_dict_to_yaml(hint.config_change['change'])}")
                lines.append("```")
            
            lines.append("")
        
        return "\n".join(lines)


def _dict_to_yaml(d: dict, indent: int = 0) -> str:
    """Simple dict to YAML conversion."""
    lines = []
    prefix = "  " * indent
    for k, v in d.items():
        if isinstance(v, dict):
            lines.append(f"{prefix}{k}:")
            lines.append(_dict_to_yaml(v, indent + 1))
        else:
            lines.append(f"{prefix}{k}: {v}")
    return "\n".join(lines)
```
```

---

## Summary

This framework provides:

1. **Comprehensive taxonomy**: 6 layers, 18 sub-dimensions covering all sources of uncertainty in data analytics

2. **Structured entropy objects**: Consistent schema for measuring, storing, and communicating entropy

3. **YAML-based configuration**: Declarative approach to influence entropy detection and set expectations

4. **Query agent integration**: Context generation that enables LLMs to handle uncertainty appropriately

5. **Resolution hints**: Prioritized, actionable guidance for reducing entropy

The key insight is that entropy should be **computable** (automated detection), **consumable** (by both LLMs and humans), and **actionable** (clear path to resolution).
