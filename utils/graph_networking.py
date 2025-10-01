from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from .aux import env_or_fail 

def initializeClient() -> GraphServiceClient:
    tenant_id = env_or_fail("TENANT_ID")
    client_id = env_or_fail("CLIENT_ID")
    client_secret = env_or_fail("CLIENT_SECRET")

    print(tenant_id)
    print(client_id)
    print(client_secret)
    cred = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    scopes = ['https://graph.microsoft.com/.default']

    client = GraphServiceClient(credentials=cred, scopes=scopes)
    return client

async def list_all_drives(client: GraphServiceClient, site_id: str) -> list[str]:
    resp = await client.sites.by_site_id(site_id).drives.get()
    drives = resp.value or []
    return [d.name for d in drives if getattr(d, "name", None)]
