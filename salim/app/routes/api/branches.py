from fastapi import APIRouter, Query
from ...utils.stores import STORES_DATA

router = APIRouter(prefix="/branches", tags=["branches"])


@router.get("/list_all_branches", summary="branches of all supermarkets")
def list_all_branches():
    items = []
    for provider in STORES_DATA.values():
        provider_name = provider["provider_name"]
        branches = provider.get("branches", {})
        for bdata in branches.values():
            items.append({
                "provider": provider_name,
                "name": bdata.get("name", ""),
                "address": bdata.get("address", ""),
                "city": bdata.get("city", "")
            })
    return {"items": items}


@router.get("/branch_by_name", summary="branches of a certain supermarket", description="return all branches of a certain provider by name\n\nproviders names:\n- קרפור\n- יוחננוף\n- אושר עד\n- קשת טעמים\n- פוליצר\n- טיב טעם")
def list_all_branches(provider: str = Query(None, description="provider name")):
    items = []

    for prov in STORES_DATA.values():
        provider_name = prov["provider_name"]

        if provider and provider_name != provider:
            continue

        for bdata in prov.get("branches", {}).values():
            items.append({
                "provider": provider_name,
                "name": bdata.get("name", ""),
                "address": bdata.get("address", ""),
                "city": bdata.get("city", "")
            })

    return {"items": items}
