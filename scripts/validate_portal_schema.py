import json, os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.ai.contentunderstanding import ContentUnderstandingClient

load_dotenv(".env", override=True)
ep = os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
cred = ClientSecretCredential(tenant_id=os.environ["AZURE_TENANT_ID"], client_id=os.environ["AZURE_CLIENT_ID"], client_secret=os.environ["AZURE_CLIENT_SECRET"])
client = ContentUnderstandingClient(endpoint=ep, credential=cred)

with open("analyzers/sec_financial_tables_v1_portal.json", "r", encoding="utf-8") as f:
    tmpl = json.load(f)
tmpl["models"] = {"completion": os.environ["GPT41_MODEL_DEPLOYMENT"], "embedding": os.environ["EMBEDDING_MODEL_DEPLOYMENT"]}

poller = client.begin_create_analyzer(analyzer_id="sec_ft_portal_test", resource=tmpl, allow_replace=True)
result = poller.result()
print("OK")
client.delete_analyzer("sec_ft_portal_test")
print("deleted")
