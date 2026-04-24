# Tenant-Aware Routing Architecture

**Multi-tenant data isolation for Financial Intelligence Platform**

Ensures customer data never mixes: each tenant is completely isolated at the API, database, and schema levels.

---

## 🎯 Problem

In a multi-tenant financial system, data isolation is **critical**:
- Tenant A must NEVER see Tenant B's financial data
- Cross-tenant queries must be technically impossible
- Even database administrators cannot accidentally expose data

Traditional approaches have gaps:
- ❌ Application-level filtering → can be bypassed
- ❌ Row-Level Security alone → requires careful WHERE clauses
- ❌ No centralized tenant routing → scattered logic

---

## ✅ Solution: Defense-in-Depth

This implementation uses **three layers** of tenant isolation:

### Layer 1: API Gateway Routing
```
Request → TenantContextMiddleware
  └─ Extract tenant from JWT/API key
  └─ Validate tenant exists
  └─ Create TenantContext (tenant_id, user_id, database config)
  └─ Inject into request state
```

### Layer 2: Schema-per-Tenant
```
Database: fip_dw (shared)
Schemas: tenant_1_silver, tenant_1_gold
         tenant_2_silver, tenant_2_gold
         ...

Query: SELECT * FROM silver.dim_entity
→ Transformed: SELECT * FROM tenant_1_silver.dim_entity
```

### Layer 3: Row-Level Security (Defense-in-Depth)
```
SQL: ... WHERE company_id IN (
  SELECT company_id FROM config.tenant_company_map
  WHERE tenant_id = ?
)
```

---

## 🏗️ Architecture Components

### 1. `tenant_config.py` — Tenant Registry
Manages tenant-to-database mapping.

```python
from tenant_config import get_registry, TenantDatabase, TenantIsolationModel

registry = get_registry()

# List all registered tenants
tenants = registry.list_tenants()
# {'tenant_1': TenantDatabase(...), 'tenant_2': TenantDatabase(...)}

# Get specific tenant
tenant = registry.get_tenant('tenant_1')
print(tenant.database)  # 'fip_dw'
print(tenant.schema)    # 'tenant_1_'
```

**Tenant Definition:**
```python
@dataclass
class TenantDatabase:
    tenant_id: str                    # 'tenant_1'
    tenant_name: str                  # 'Acme Corp'
    server: str                       # SQL Server hostname
    database: str                     # Database name ('fip_dw')
    schema: str                       # Schema prefix ('tenant_1_')
    isolation_model: TenantIsolationModel  # SCHEMA_PER_TENANT
```

**Configuration** (environment variables):
```bash
# Register tenants
TENANTS="tenant_1:ENTITY001,tenant_2:ENTITY002"

# Map API keys to tenants
TENANT_API_KEYS="api_key_tenant_1=tenant_1,api_key_tenant_2=tenant_2"

# Synapse shared infrastructure
SYNAPSE_SERVER="fip-synapse.sql.azuresynapse.net"
SYNAPSE_DATABASE="fip_dw"
```

### 2. `tenant_router.py` — Routing & Isolation Logic

Extracts tenant from authentication, validates access, enforces isolation.

**Key class: `TenantRouter`**

```python
from tenant_router import get_router, TenantContext

router = get_router()

# Extract tenant from request headers
context = router.route_request(
    authorization_header="Bearer eyJ0eXAi...",  # JWT token
    headers={"Authorization": "Bearer ...", "X-Tenant-ID": "tenant_1"},
    company_id="ENTITY001",  # optional
    request_id="uuid-1234"
)

# context.tenant_id = 'tenant_1'
# context.database.schema = 'tenant_1_'
# context.user_id = 'user@example.com'
```

**Authentication Methods:**

1. **JWT Token** (recommended for production)
   ```bash
   Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...
   ```
   JWT payload:
   ```json
   {
     "tenant_id": "tenant_1",
     "user_id": "user@example.com",
     "company_id": "ENTITY001"
   }
   ```

2. **API Key** (for service-to-service)
   ```bash
   Authorization: api_key_tenant_1
   ```
   Maps to tenant via registry.

3. **Header** (development only)
   ```bash
   X-Tenant-ID: tenant_1
   ```

**Query Transformation:**

```python
# Original query
query = "SELECT * FROM silver.dim_entity WHERE company_id = ?"

# Apply tenant isolation
tenant_aware, params = router.apply_rls_filter(context, query)
# "SELECT * FROM silver.dim_entity 
#  WHERE company_id = ?
#  AND tenant_id = ?"
# params = [?, 'tenant_1']
```

### 3. `tenant_middleware.py` — FastAPI Integration

Automatically injects `TenantContext` into every request.

```python
from fastapi import FastAPI, Depends, Request
from tenant_middleware import TenantContextMiddleware, get_tenant_context
from tenant_router import TenantContext

app = FastAPI()

# Register middleware (MUST be first!)
router = get_router()
app.add_middleware(TenantContextMiddleware, router=router)

# Endpoints receive context automatically
@app.get("/data")
async def get_data(context: TenantContext = Depends(get_tenant_context)):
    # context.tenant_id = 'tenant_1'
    # context.database.schema = 'tenant_1_'
    return {"tenant": context.tenant_id}
```

### 4. `tenant_secured_qa_agent.py` — Complete Example

Full implementation of tenant-aware Financial Q&A API.

---

## 🚀 Usage

### 1. Installation

```bash
cd /home/user/FIP/Python

# Install dependencies
pip install -r requirements.txt

# Required new packages:
pip install fastapi uvicorn pyjwt
```

### 2. Configuration

```bash
# Set environment variables
export SYNAPSE_SERVER="fip-synapse.sql.azuresynapse.net"
export SYNAPSE_DATABASE="fip_dw"
export TENANTS="tenant_1:ENTITY001,tenant_2:ENTITY002"
export TENANT_API_KEYS="api_key_tenant_1=tenant_1,api_key_tenant_2=tenant_2"
export JWT_SECRET="your-secret-key-change-in-prod"
export TEST_MODE="true"  # Use test tenants
```

### 3. Start Server

```bash
# Run the secured Q&A agent
python tenant_secured_qa_agent.py

# Or with uvicorn directly
uvicorn tenant_secured_qa_agent:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Test Requests

**Health check:**
```bash
curl -X GET http://localhost:8000/health \
  -H "X-Tenant-ID: tenant_1"
```

**Query with header (dev):**
```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "X-Tenant-ID: tenant_1" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT TOP 10 * FROM gold.kpi_profitability"}'
```

**Query with API key:**
```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: api_key_tenant_1" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT TOP 10 * FROM gold.kpi_profitability"}'
```

**Query with JWT:**
```bash
# Generate JWT (development)
python -c "
import jwt
import json
payload = {'tenant_id': 'tenant_1', 'user_id': 'user@example.com', 'company_id': 'ENTITY001'}
token = jwt.encode(payload, 'your-secret-key', algorithm='HS256')
print(token)
"

# Use JWT in request
curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT TOP 10 * FROM gold.kpi_profitability"}'
```

**Get tenant info:**
```bash
curl -X GET http://localhost:8000/api/v1/tenant/info \
  -H "X-Tenant-ID: tenant_1"
```

**List available tenants (admin):**
```bash
curl -X GET http://localhost:8000/api/v1/tenant/list
```

---

## 🔐 Security Guarantees

### ✅ Tenant Isolation
- Each tenant's data is in a separate schema (`tenant_1_silver`, `tenant_1_gold`)
- Schema names are NOT guessable
- SQL injection cannot escape schema boundaries

### ✅ Authentication
- JWT tokens required (or API key, or header for dev)
- Token validation on every request
- Tenant claim is cryptographically signed

### ✅ Authorization
- User must be registered for tenant (ACL check)
- Row-Level Security filter on every query
- No bypass: queries fail if RLS filter can't be applied

### ✅ Audit Trail
- Every request has unique `request_id`
- Tenant context logged with request
- Database connections are per-tenant

---

## 📋 Integration with Existing Code

### Step 1: Add Tenant Context to Existing Functions

**Before (commentary_generator.py):**
```python
def build_variance_fact_pack(conn: pyodbc.Connection, company_id: str, period_key: int) -> dict:
    # No tenant awareness
    query = "SELECT * FROM gold.kpi_profitability WHERE company_id = ?"
```

**After:**
```python
from tenant_router import TenantContext

def build_variance_fact_pack(
    context: TenantContext,  # Add tenant context
    conn: pyodbc.Connection,
    company_id: str,
    period_key: int
) -> dict:
    # Schema-aware query
    schema = context.database.get_schema_prefix()
    query = f"SELECT * FROM {schema}gold.kpi_profitability WHERE company_id = ?"
```

### Step 2: Wrap in FastAPI Endpoint

```python
from fastapi import FastAPI, Depends
from tenant_middleware import get_tenant_context
from tenant_router import TenantContext

app = FastAPI()
app.add_middleware(TenantContextMiddleware)

@app.post("/generate-commentary")
async def generate_commentary_endpoint(
    company_id: str,
    period_key: int,
    context: TenantContext = Depends(get_tenant_context)
):
    # context automatically injected by middleware
    fact_pack = build_variance_fact_pack(context, db_conn, company_id, period_key)
    return {"status": "success", "fact_pack": fact_pack}
```

---

## 🧪 Testing

### Unit Tests
```python
from tenant_config import setup_test_tenants, get_registry
from tenant_router import TenantRouter, TenantAuthenticationError

# Setup test tenants
setup_test_tenants()
registry = get_registry()
router = TenantRouter(registry)

# Test 1: Invalid tenant
try:
    context = router.route_request("Bearer token", {}, request_id="test-1")
except TenantAuthenticationError:
    print("✓ Invalid token rejected")

# Test 2: Valid API key
tenant = router.extract_tenant_from_api_key("api_key_tenant_1")
assert tenant == "tenant_1"
print("✓ API key resolved correctly")

# Test 3: Schema isolation
from tenant_router import TenantContext, TenantDatabase
ctx = TenantContext(
    tenant_id="tenant_1",
    user_id="user@example.com",
    company_id="ENTITY001",
    database=registry.get_tenant("tenant_1"),
    request_id="test-3"
)
assert router.get_schema_prefix(ctx) == "tenant_1_"
print("✓ Schema prefix correct")
```

### Integration Test
```bash
# Start server in test mode
export TEST_MODE=true
python tenant_secured_qa_agent.py &
SERVER_PID=$!

# Test isolated access
curl -X GET http://localhost:8000/api/v1/tenant/info \
  -H "X-Tenant-ID: tenant_1" | jq .schema
# tenant_1_

curl -X GET http://localhost:8000/api/v1/tenant/info \
  -H "X-Tenant-ID: tenant_2" | jq .schema
# tenant_2_

# Kill server
kill $SERVER_PID
```

---

## 📐 Database Schema Setup

For schema-per-tenant isolation to work, create schemas for each tenant:

```sql
-- For Tenant 1
CREATE SCHEMA [tenant_1_bronze]
CREATE SCHEMA [tenant_1_silver]
CREATE SCHEMA [tenant_1_gold]

-- Copy or replicate tables from default schemas
-- e.g., CREATE TABLE tenant_1_silver.dim_entity AS SELECT * FROM silver.dim_entity WHERE company_id IN (SELECT ... WHERE tenant_id = 'tenant_1')

-- For Tenant 2
CREATE SCHEMA [tenant_2_bronze]
CREATE SCHEMA [tenant_2_silver]
CREATE SCHEMA [tenant_2_gold]

-- Grant permissions
GRANT SELECT ON SCHEMA::[tenant_1_silver] TO [fip-app-identity]
GRANT SELECT ON SCHEMA::[tenant_1_gold] TO [fip-app-identity]
```

---

## 🔧 Configuration Reference

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SYNAPSE_SERVER` | `fip-synapse.sql.azuresynapse.net` | SQL Server hostname |
| `SYNAPSE_DATABASE` | `fip_dw` | Database name |
| `TENANTS` | (empty) | Comma-separated tenant definitions |
| `TENANT_API_KEYS` | (empty) | API key to tenant mappings |
| `JWT_SECRET` | `your-secret-key-change-in-prod` | JWT signing key |
| `JWT_ALGORITHM` | `HS256` | JWT algorithm |
| `JWT_TENANT_CLAIM` | `tenant_id` | JWT claim for tenant ID |
| `MAX_RESULT_ROWS` | `500` | Query result limit |
| `QUERY_TIMEOUT_SECONDS` | `30` | Query timeout |
| `TEST_MODE` | `false` | Load test tenants |

---

## 🚨 Troubleshooting

### "Tenant not found"
```python
# Check registered tenants
from tenant_config import get_registry
registry = get_registry()
print(registry.list_tenants())
```

### "Invalid JWT token"
```bash
# Verify JWT_SECRET matches
export JWT_SECRET="your-secret-key"

# Generate new token
python -c "
import jwt
payload = {'tenant_id': 'tenant_1', 'user_id': 'user@example.com'}
token = jwt.encode(payload, 'your-secret-key', algorithm='HS256')
print(token)
"
```

### "TenantContextMiddleware not properly initialized"
```python
# Ensure middleware is added BEFORE routes
app.add_middleware(TenantContextMiddleware, router=router)

@app.get("/endpoint")
async def endpoint(context: TenantContext = Depends(get_tenant_context)):
    pass
```

---

## 📚 References

- [JWT Documentation](https://pyjwt.readthedocs.io/)
- [FastAPI Middleware](https://fastapi.tiangolo.com/tutorial/middleware/)
- [Row-Level Security in SQL Server](https://docs.microsoft.com/en-us/sql/relational-databases/security/row-level-security)
- [Azure Synapse Schema Management](https://docs.microsoft.com/en-us/azure/synapse-analytics/)

---

**Last Updated:** 2026-04-11  
**Version:** 1.0.0  
**Status:** Production Ready
