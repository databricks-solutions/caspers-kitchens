# Databricks notebook source
import mlflow
mlflow.set_registry_uri("databricks-uc")
print("=== mlflow.genai.search_prompts under oleksandra.prompts ===")
try:
    res = mlflow.genai.search_prompts(filter_string="catalog_name = 'oleksandra' AND schema_name = 'prompts'", max_results=50)
    print(f"count: {len(res) if hasattr(res,'__len__') else 'n/a'}")
    for p in (res or [])[:20]:
        print(f"  name={getattr(p,'name','?')}")
except Exception as e:
    print(f"search_prompts error: {type(e).__name__}: {e}")
print("---")
print("=== load_prompt for known names ===")
for name in [
    "oleksandra.prompts.refund_system",
    "oleksandra.prompts.operational_supervisor_system",
    "oleksandra.prompts.ka_inspection_knowledge_system",
    "oleksandra.prompts.ka_menu_knowledge_system",
    "oleksandra.prompts.ka_legal_system",
    "oleksandra.prompts.ka_audits_system",
    "oleksandra.prompts.ka_consultancy_system",
    "oleksandra.prompts.ka_regulatory_system",
]:
    try:
        p = mlflow.genai.load_prompt(name_or_uri=f"prompts:/{name}@production")
        print(f"  ✅ {name}  v{p.version}  ({len(p.template)} chars)")
    except Exception as e:
        print(f"  ❌ {name}: {type(e).__name__}: {str(e)[:200]}")
