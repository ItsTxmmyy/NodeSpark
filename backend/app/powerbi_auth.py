"""
Power BI embedding authentication using Azure AD (Microsoft Entra ID).

Prerequisites:
1. Register an app in Azure AD (https://portal.azure.com/#view/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/RegisteredApps)
2. Grant API permission: "Power BI Service" -> "Report.Read.All"
3. Create a client secret
4. Set environment variables below

Environment Variables:
- POWERBI_TENANT_ID: Your Azure AD tenant ID
- POWERBI_CLIENT_ID: Your app's client ID
- POWERBI_CLIENT_SECRET: Your app's client secret
- POWERBI_WORKSPACE_ID: (Optional) Specific workspace ID for embedded reports
"""

import os
import time
from typing import Optional

import msal
import requests

# Azure AD token cache
_token_cache: Optional[msal.TokenCache] = None
_cached_token: Optional[dict] = None


def _get_token_cache() -> msal.TokenCache:
    """Get or create the token cache."""
    global _token_cache
    if _token_cache is None:
        _token_cache = msal.TokenCache()
    return _token_cache


def get_access_token() -> str:
    """
    Acquire an Azure AD access token for Power BI API.
    Uses client credentials flow for service-to-service authentication.
    """
    global _cached_token
    
    tenant_id = os.getenv("POWERBI_TENANT_ID")
    client_id = os.getenv("POWERBI_CLIENT_ID")
    client_secret = os.getenv("POWERBI_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "Missing required environment variables: "
            "POWERBI_TENANT_ID, POWERBI_CLIENT_ID, POWERBI_CLIENT_SECRET"
        )
    
    # Check if we have a valid cached token
    if _cached_token and _cached_token.get("expires_in"):
        expires_at = _cached_token.get("expires_at", 0)
        if time.time() < expires_at - 60:  # 60 second buffer
            return _cached_token["access_token"]
    
    # Build MSAL application
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        token_cache=_get_token_cache()
    )
    
    # Request token for Power BI API
    scopes = ["https://analysis.windows.net/powerbi/api/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    
    if "access_token" not in result:
        error = result.get("error", "unknown")
        description = result.get("error_description", "No description")
        raise RuntimeError(f"Failed to acquire token: {error} - {description}")
    
    # Cache the token with expiration time
    _cached_token = result
    _cached_token["expires_at"] = time.time() + result.get("expires_in", 3600)
    
    return result["access_token"]


def get_embed_token(report_id: str, workspace_id: Optional[str] = None) -> str:
    """
    Get an embed token for a specific Power BI report.
    
    Args:
        report_id: The Power BI report ID
        workspace_id: Optional workspace ID (if not using My Workspace)
    
    Returns:
        Embed token string
    """
    access_token = get_access_token()
    
    # Power BI API endpoint for generating embed tokens
    api_url = "https://api.powerbi.com/v2.0/myorg/embedTokens"
    
    # Build the request body
    datasets = []
    reports = [{"id": report_id}]
    target_workspaces = [workspace_id] if workspace_id else []
    
    payload = {
        "datasets": datasets,
        "reports": reports,
        "targetWorkspaces": target_workspaces
    }
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(api_url, json=payload, headers=headers)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to get embed token: {response.status_code} - {response.text}")
    
    result = response.json()
    
    if "token" not in result:
        raise RuntimeError(f"No embed token in response: {result}")
    
    return result["token"]


def get_report_embed_config(report_id: str, embed_url: str) -> dict:
    """
    Get complete embed configuration for a Power BI report.
    
    Args:
        report_id: The Power BI report ID
        embed_url: The embed URL for the report
    
    Returns:
        Embed configuration dictionary for the Power BI JavaScript SDK
    """
    embed_token = get_embed_token(report_id)
    
    return {
        "type": "report",
        "id": report_id,
        "embedUrl": embed_url,
        "accessToken": embed_token,
        "tokenType": 2,  # 2 = Embed token (not Aad token)
        "settings": {
            "filterPaneEnabled": False,
            "navContentPaneEnabled": True,
            "background": 0  # Transparent background
        }
    }