"""HashiCorp Vault client for secrets management.

Provides interface to retrieve and manage secrets from Vault.
Supports automatic token renewal and dynamic secrets.
"""

import os
import threading
import time
from typing import Any, Dict, Optional

import hvac
from hvac.exceptions import VaultError


class VaultClient:
    """HashiCorp Vault client wrapper."""

    def __init__(
        self,
        vault_url: str = "http://vault:8200",
        vault_token: Optional[str] = None,
        role_id: Optional[str] = None,
        secret_id: Optional[str] = None,
    ) -> None:
        """Initialize Vault client.

        Args:
            vault_url: Vault server URL
            vault_token: Vault token (for token auth)
            role_id: Role ID (for AppRole auth)
            secret_id: Secret ID (for AppRole auth)
        """
        self.vault_url = vault_url
        self.client = hvac.Client(url=vault_url)
        self.lease_renewal_thread: Optional[threading.Thread] = None
        self.active_leases: Dict[str, str] = {}
        self._stop_renewal = threading.Event()

        # Authenticate
        if vault_token:
            self.client.token = vault_token
        elif role_id and secret_id:
            self._authenticate_approle(role_id, secret_id)
        else:
            # Try to get token from environment
            token = os.getenv("VAULT_TOKEN")
            if token:
                self.client.token = token
            else:
                raise ValueError("No authentication method provided for Vault")

        # Start lease renewal thread
        self._start_lease_renewal()

    def _authenticate_approle(self, role_id: str, secret_id: str) -> None:
        """Authenticate using AppRole method.

        Args:
            role_id: AppRole role ID
            secret_id: AppRole secret ID
        """
        try:
            response = self.client.auth.approle.login(
                role_id=role_id, secret_id=secret_id
            )
            self.client.token = response["auth"]["client_token"]
        except VaultError as e:
            raise RuntimeError(f"Failed to authenticate with Vault: {e}")

    def get_secret(self, path: str, mount_point: str = "secret") -> Dict[str, Any]:
        """Get secret from Vault KV store.

        Args:
            path: Secret path (e.g., "mongodb/credentials")
            mount_point: KV mount point (default: "secret")

        Returns:
            Secret data dictionary
        """
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=path, mount_point=mount_point
            )
            return response["data"]["data"]
        except VaultError as e:
            raise RuntimeError(f"Failed to read secret {path}: {e}")

    def get_dynamic_credentials(
        self, role_name: str, database: str = "mongodb"
    ) -> Dict[str, Any]:
        """Get dynamic database credentials from Vault.

        Args:
            role_name: Database role name
            database: Database backend name (default: "mongodb")

        Returns:
            Dictionary with username, password, and lease_id
        """
        try:
            response = self.client.secrets.database.generate_credentials(
                name=role_name, mount_point=database
            )

            credentials = {
                "username": response["data"]["username"],
                "password": response["data"]["password"],
                "lease_id": response["lease_id"],
                "lease_duration": response["lease_duration"],
            }

            # Track lease for renewal
            self.active_leases[response["lease_id"]] = role_name

            return credentials
        except VaultError as e:
            raise RuntimeError(f"Failed to generate credentials for {role_name}: {e}")

    def renew_lease(self, lease_id: str, increment: Optional[int] = None) -> None:
        """Renew a Vault lease.

        Args:
            lease_id: Lease ID to renew
            increment: Optional renewal increment in seconds
        """
        try:
            self.client.sys.renew_lease(lease_id=lease_id, increment=increment)
        except VaultError as e:
            # Lease expired or invalid, remove from tracking
            if lease_id in self.active_leases:
                del self.active_leases[lease_id]
            raise RuntimeError(f"Failed to renew lease {lease_id}: {e}")

    def revoke_lease(self, lease_id: str) -> None:
        """Revoke a Vault lease.

        Args:
            lease_id: Lease ID to revoke
        """
        try:
            self.client.sys.revoke_lease(lease_id=lease_id)
            if lease_id in self.active_leases:
                del self.active_leases[lease_id]
        except VaultError as e:
            raise RuntimeError(f"Failed to revoke lease {lease_id}: {e}")

    def _start_lease_renewal(self) -> None:
        """Start background thread for automatic lease renewal."""

        def renewal_loop() -> None:
            while not self._stop_renewal.is_set():
                for lease_id in list(self.active_leases.keys()):
                    try:
                        self.renew_lease(lease_id)
                    except RuntimeError:
                        # Lease renewal failed, will be handled by caller
                        pass
                time.sleep(300)  # Renew every 5 minutes

        self.lease_renewal_thread = threading.Thread(target=renewal_loop, daemon=True)
        self.lease_renewal_thread.start()

    def close(self) -> None:
        """Close Vault client and revoke all leases."""
        self._stop_renewal.set()
        if self.lease_renewal_thread:
            self.lease_renewal_thread.join(timeout=5)

        # Revoke all active leases
        for lease_id in list(self.active_leases.keys()):
            try:
                self.revoke_lease(lease_id)
            except RuntimeError:
                pass

    def __enter__(self) -> "VaultClient":
        """Context manager entry.

        Returns:
            Self
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


_vault_client_instance: Optional[VaultClient] = None


def get_vault_client(
    vault_url: Optional[str] = None,
    vault_token: Optional[str] = None,
    role_id: Optional[str] = None,
    secret_id: Optional[str] = None,
) -> VaultClient:
    """Get or create singleton Vault client instance.

    Args:
        vault_url: Vault server URL
        vault_token: Vault token
        role_id: AppRole role ID
        secret_id: AppRole secret ID

    Returns:
        VaultClient instance
    """
    global _vault_client_instance

    if _vault_client_instance is None:
        _vault_client_instance = VaultClient(
            vault_url=vault_url or os.getenv("VAULT_ADDR", "http://vault:8200"),
            vault_token=vault_token,
            role_id=role_id,
            secret_id=secret_id,
        )

    return _vault_client_instance
