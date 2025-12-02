"""
Admin router for user management and authentication.

Provides REST API endpoints for:
- User authentication (login)
- User management (CRUD operations)
- Role management
- Admin-only operations

All admin endpoints require authentication and appropriate permissions.
"""

import structlog
from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Request

from api.src.models.auth import (
    LoginRequest, TokenResponse, UserResponse,
    CreateUserRequest, UpdateUserRequest,
    ErrorResponse, CurrentUser
)
from api.src.models.audit import AuditAction, ResourceType
from api.src.repositories.user_repo import UserRepository
from api.src.repositories.audit_repo import AuditRepository
from api.src.services.auth_service import AuthService
from api.src.dependencies import (
    get_user_repository,
    get_audit_repository,
    get_auth_service,
    get_auth_service_with_db,
    get_current_active_user,
    require_admin,
    get_pagination_params,
    PaginationParams,
    get_client_ip,
    get_user_agent
)

logger = structlog.get_logger(__name__)

# Create router for authentication endpoints
auth_router = APIRouter(
    prefix="/auth",
    tags=["Authentication"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        422: {"model": ErrorResponse, "description": "Validation Error"}
    }
)

# Create router for admin endpoints
admin_router = APIRouter(
    prefix="/admin",
    tags=["User Management"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        403: {"model": ErrorResponse, "description": "Forbidden"},
        404: {"model": ErrorResponse, "description": "Not Found"},
        422: {"model": ErrorResponse, "description": "Validation Error"}
    }
)


# ============================================================================
# AUTHENTICATION ENDPOINTS
# ============================================================================


@auth_router.post(
    "/login",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
    summary="User Login",
    description="""
    Authenticate user with username and password.

    Returns JWT access token on successful authentication.

    **Authentication:** Not required (public endpoint)

    **Request Body:**
    - username: User's username (3-50 characters)
    - password: User's password (minimum 8 characters)

    **Success Response (200):**
    - access_token: JWT token for authentication
    - token_type: "bearer"
    - expires_in: Token expiration time in seconds

    **Error Responses:**
    - 401: Invalid credentials or inactive user
    - 422: Validation error (invalid request format)
    """,
    responses={
        200: {
            "description": "Login successful",
            "model": TokenResponse
        },
        401: {
            "description": "Invalid credentials",
            "model": ErrorResponse,
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid credentials", "error_code": "AUTH_001"}
                }
            }
        }
    }
)
async def login(
    request: Request,
    login_request: LoginRequest,
    auth_service: AuthService = Depends(get_auth_service),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    client_ip: str = Depends(get_client_ip),
    user_agent: Optional[str] = Depends(get_user_agent)
) -> TokenResponse:
    """
    Authenticate user and return JWT token.

    Args:
        request: HTTP request
        login_request: Login credentials
        auth_service: Authentication service
        audit_repo: Audit repository
        client_ip: Client IP address
        user_agent: Client user agent

    Returns:
        JWT token response

    Raises:
        HTTPException: If authentication fails
    """
    try:
        logger.info(
            "login_attempt",
            username=login_request.username,
            ip_address=client_ip
        )

        # Authenticate user
        token_response = await auth_service.login(login_request)

        if not token_response:
            # Log failed login attempt
            await audit_repo.create_audit_log(
                user_id=None,
                action=AuditAction.LOGIN_FAILURE.value,
                resource_type=ResourceType.AUTH.value,
                resource_id=login_request.username,
                details={"reason": "invalid_credentials"},
                ip_address=client_ip,
                user_agent=user_agent,
                status_code=status.HTTP_401_UNAUTHORIZED
            )

            logger.warning(
                "login_failed",
                username=login_request.username,
                ip_address=client_ip
            )

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Bearer"}
            )

        # Get user for audit log
        user = await auth_service.authenticate_user(login_request)
        if user:
            # Log successful login
            await audit_repo.create_audit_log(
                user_id=user.id,
                action=AuditAction.LOGIN_SUCCESS.value,
                resource_type=ResourceType.AUTH.value,
                resource_id=str(user.id),
                details={"username": user.username},
                ip_address=client_ip,
                user_agent=user_agent,
                status_code=status.HTTP_200_OK
            )

        logger.info(
            "login_success",
            username=login_request.username,
            ip_address=client_ip
        )

        return token_response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "login_error",
            error=str(e),
            username=login_request.username,
            ip_address=client_ip
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Login failed"
        )


# ============================================================================
# USER MANAGEMENT ENDPOINTS (ADMIN ONLY)
# ============================================================================


@admin_router.post(
    "/users",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create User",
    description="""
    Create a new user account.

    **Authentication:** Required (admin role)

    **Permissions:** Admin only

    **Request Body:**
    - username: Unique username (3-50 characters)
    - email: Valid email address
    - password: Secure password (minimum 8 characters)
    - roles: List of role names (default: ["analyst"])

    **Success Response (201):**
    Returns created user details (without password hash)

    **Error Responses:**
    - 401: Not authenticated
    - 403: Not authorized (admin role required)
    - 409: Username or email already exists
    - 422: Validation error
    """,
    responses={
        201: {
            "description": "User created successfully",
            "model": UserResponse
        },
        409: {
            "description": "Username or email already exists",
            "model": ErrorResponse,
            "content": {
                "application/json": {
                    "example": {"detail": "Username already exists", "error_code": "USER_001"}
                }
            }
        }
    }
)
async def create_user(
    request: Request,
    create_request: CreateUserRequest,
    admin: CurrentUser = Depends(require_admin),
    user_repo: UserRepository = Depends(get_user_repository),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    auth_service: AuthService = Depends(get_auth_service_with_db),
    client_ip: str = Depends(get_client_ip),
    user_agent: Optional[str] = Depends(get_user_agent)
) -> UserResponse:
    """
    Create a new user.

    Args:
        request: HTTP request
        create_request: User creation details
        admin: Admin user (from dependency)
        user_repo: User repository
        audit_repo: Audit repository
        auth_service: Authentication service
        client_ip: Client IP address
        user_agent: Client user agent

    Returns:
        Created user details

    Raises:
        HTTPException: If creation fails
    """
    try:
        logger.info(
            "user_create_attempt",
            admin_id=str(admin.id),
            admin_username=admin.username,
            username=create_request.username,
            email=create_request.email,
            roles=create_request.roles
        )

        # Hash password
        password_hash = auth_service.hash_password(create_request.password)

        # Create user
        user = await user_repo.create_user(
            username=create_request.username,
            email=create_request.email,
            password_hash=password_hash,
            roles=create_request.roles,
            is_active=True
        )

        # Get user roles for response
        roles = await user_repo.get_user_roles(user.id)

        # Log audit event
        await audit_repo.create_audit_log(
            user_id=admin.id,
            action=AuditAction.USER_CREATE.value,
            resource_type=ResourceType.USER.value,
            resource_id=str(user.id),
            details={
                "username": user.username,
                "email": user.email,
                "roles": roles,
                "created_by": admin.username
            },
            ip_address=client_ip,
            user_agent=user_agent,
            status_code=status.HTTP_201_CREATED
        )

        logger.info(
            "user_created",
            user_id=str(user.id),
            username=user.username,
            created_by=admin.username
        )

        return user.to_response(roles)

    except ValueError as e:
        # Username or email already exists
        logger.warning(
            "user_create_conflict",
            error=str(e),
            username=create_request.username,
            email=create_request.email
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            "user_create_error",
            error=str(e),
            username=create_request.username
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )


@admin_router.get(
    "/users",
    response_model=List[UserResponse],
    status_code=status.HTTP_200_OK,
    summary="List Users",
    description="""
    Get list of users with pagination.

    **Authentication:** Required (admin role)

    **Permissions:** Admin only

    **Query Parameters:**
    - limit: Maximum number of users to return (1-1000, default: 100)
    - offset: Number of users to skip (default: 0)
    - is_active: Filter by active status (optional)

    **Success Response (200):**
    Returns array of user objects

    **Error Responses:**
    - 401: Not authenticated
    - 403: Not authorized (admin role required)
    """,
    responses={
        200: {
            "description": "List of users",
            "model": List[UserResponse]
        }
    }
)
async def list_users(
    request: Request,
    admin: CurrentUser = Depends(require_admin),
    user_repo: UserRepository = Depends(get_user_repository),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    pagination: PaginationParams = Depends(get_pagination_params),
    is_active: Optional[bool] = None,
    client_ip: str = Depends(get_client_ip),
    user_agent: Optional[str] = Depends(get_user_agent)
) -> List[UserResponse]:
    """
    List users with pagination.

    Args:
        request: HTTP request
        admin: Admin user (from dependency)
        user_repo: User repository
        audit_repo: Audit repository
        pagination: Pagination parameters
        is_active: Filter by active status
        client_ip: Client IP address
        user_agent: Client user agent

    Returns:
        List of users
    """
    try:
        logger.info(
            "user_list_attempt",
            admin_id=str(admin.id),
            admin_username=admin.username,
            limit=pagination.limit,
            offset=pagination.offset,
            is_active=is_active
        )

        # Get users
        users = await user_repo.list_users(
            limit=pagination.limit,
            offset=pagination.offset,
            is_active=is_active
        )

        # Get roles for each user
        user_responses = []
        for user in users:
            roles = await user_repo.get_user_roles(user.id)
            user_responses.append(user.to_response(roles))

        # Log audit event
        await audit_repo.create_audit_log(
            user_id=admin.id,
            action=AuditAction.USER_LIST.value,
            resource_type=ResourceType.USER.value,
            details={
                "count": len(users),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "is_active": is_active
            },
            ip_address=client_ip,
            user_agent=user_agent,
            status_code=status.HTTP_200_OK
        )

        logger.info(
            "user_list_success",
            count=len(users),
            admin_username=admin.username
        )

        return user_responses

    except Exception as e:
        logger.error(
            "user_list_error",
            error=str(e),
            admin_username=admin.username
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list users"
        )


@admin_router.get(
    "/users/{user_id}",
    response_model=UserResponse,
    status_code=status.HTTP_200_OK,
    summary="Get User",
    description="""
    Get user details by ID.

    **Authentication:** Required (admin role)

    **Permissions:** Admin only

    **Path Parameters:**
    - user_id: User UUID

    **Success Response (200):**
    Returns user details

    **Error Responses:**
    - 401: Not authenticated
    - 403: Not authorized (admin role required)
    - 404: User not found
    """,
    responses={
        200: {
            "description": "User details",
            "model": UserResponse
        },
        404: {
            "description": "User not found",
            "model": ErrorResponse,
            "content": {
                "application/json": {
                    "example": {"detail": "User not found", "error_code": "USER_002"}
                }
            }
        }
    }
)
async def get_user(
    request: Request,
    user_id: str,
    admin: CurrentUser = Depends(require_admin),
    user_repo: UserRepository = Depends(get_user_repository),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    client_ip: str = Depends(get_client_ip),
    user_agent: Optional[str] = Depends(get_user_agent)
) -> UserResponse:
    """
    Get user by ID.

    Args:
        request: HTTP request
        user_id: User UUID string
        admin: Admin user (from dependency)
        user_repo: User repository
        audit_repo: Audit repository
        client_ip: Client IP address
        user_agent: Client user agent

    Returns:
        User details

    Raises:
        HTTPException: If user not found
    """
    try:
        # Validate UUID
        try:
            user_uuid = UUID(user_id)
        except ValueError:
            logger.warning("invalid_user_id_format", user_id=user_id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid user ID format"
            )

        logger.info(
            "user_get_attempt",
            admin_id=str(admin.id),
            admin_username=admin.username,
            user_id=user_id
        )

        # Get user
        user = await user_repo.get_user_by_id(user_uuid)

        if not user:
            logger.warning(
                "user_not_found",
                user_id=user_id,
                admin_username=admin.username
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Get user roles
        roles = await user_repo.get_user_roles(user.id)

        # Log audit event
        await audit_repo.create_audit_log(
            user_id=admin.id,
            action=AuditAction.USER_READ.value,
            resource_type=ResourceType.USER.value,
            resource_id=user_id,
            details={
                "username": user.username,
                "accessed_by": admin.username
            },
            ip_address=client_ip,
            user_agent=user_agent,
            status_code=status.HTTP_200_OK
        )

        logger.info(
            "user_get_success",
            user_id=user_id,
            username=user.username,
            admin_username=admin.username
        )

        return user.to_response(roles)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "user_get_error",
            error=str(e),
            user_id=user_id,
            admin_username=admin.username
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get user"
        )


@admin_router.put(
    "/users/{user_id}",
    response_model=UserResponse,
    status_code=status.HTTP_200_OK,
    summary="Update User",
    description="""
    Update user details.

    **Authentication:** Required (admin role)

    **Permissions:** Admin only

    **Path Parameters:**
    - user_id: User UUID

    **Request Body (all fields optional):**
    - email: New email address
    - password: New password (minimum 8 characters)
    - roles: New list of role names
    - is_active: Active status (true/false)

    **Success Response (200):**
    Returns updated user details

    **Error Responses:**
    - 401: Not authenticated
    - 403: Not authorized (admin role required)
    - 404: User not found
    - 409: Email already exists
    - 422: Validation error
    """,
    responses={
        200: {
            "description": "User updated successfully",
            "model": UserResponse
        },
        404: {
            "description": "User not found",
            "model": ErrorResponse
        },
        409: {
            "description": "Email already exists",
            "model": ErrorResponse
        }
    }
)
async def update_user(
    request: Request,
    user_id: str,
    update_request: UpdateUserRequest,
    admin: CurrentUser = Depends(require_admin),
    user_repo: UserRepository = Depends(get_user_repository),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    auth_service: AuthService = Depends(get_auth_service_with_db),
    client_ip: str = Depends(get_client_ip),
    user_agent: Optional[str] = Depends(get_user_agent)
) -> UserResponse:
    """
    Update user.

    Args:
        request: HTTP request
        user_id: User UUID string
        update_request: Update details
        admin: Admin user (from dependency)
        user_repo: User repository
        audit_repo: Audit repository
        auth_service: Authentication service
        client_ip: Client IP address
        user_agent: Client user agent

    Returns:
        Updated user details

    Raises:
        HTTPException: If update fails
    """
    try:
        # Validate UUID
        try:
            user_uuid = UUID(user_id)
        except ValueError:
            logger.warning("invalid_user_id_format", user_id=user_id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid user ID format"
            )

        logger.info(
            "user_update_attempt",
            admin_id=str(admin.id),
            admin_username=admin.username,
            user_id=user_id
        )

        # Hash password if provided
        password_hash = None
        if update_request.password:
            password_hash = auth_service.hash_password(update_request.password)

        # Update user
        user = await user_repo.update_user(
            user_id=user_uuid,
            email=update_request.email,
            password_hash=password_hash,
            is_active=update_request.is_active,
            roles=update_request.roles
        )

        if not user:
            logger.warning(
                "user_not_found",
                user_id=user_id,
                admin_username=admin.username
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Get user roles
        roles = await user_repo.get_user_roles(user.id)

        # Log audit event
        update_details = {}
        if update_request.email:
            update_details["email"] = update_request.email
        if update_request.password:
            update_details["password_changed"] = True
        if update_request.roles:
            update_details["roles"] = update_request.roles
        if update_request.is_active is not None:
            update_details["is_active"] = update_request.is_active

        await audit_repo.create_audit_log(
            user_id=admin.id,
            action=AuditAction.USER_UPDATE.value,
            resource_type=ResourceType.USER.value,
            resource_id=user_id,
            details={
                "username": user.username,
                "updates": update_details,
                "updated_by": admin.username
            },
            ip_address=client_ip,
            user_agent=user_agent,
            status_code=status.HTTP_200_OK
        )

        logger.info(
            "user_updated",
            user_id=user_id,
            username=user.username,
            updated_by=admin.username
        )

        return user.to_response(roles)

    except ValueError as e:
        # Email already exists
        logger.warning(
            "user_update_conflict",
            error=str(e),
            user_id=user_id
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "user_update_error",
            error=str(e),
            user_id=user_id,
            admin_username=admin.username
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )


@admin_router.delete(
    "/users/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete User",
    description="""
    Delete user (soft delete - sets is_active to false).

    **Authentication:** Required (admin role)

    **Permissions:** Admin only

    **Path Parameters:**
    - user_id: User UUID

    **Success Response (204):**
    No content (user deleted)

    **Error Responses:**
    - 401: Not authenticated
    - 403: Not authorized (admin role required)
    - 404: User not found
    """,
    responses={
        204: {
            "description": "User deleted successfully"
        },
        404: {
            "description": "User not found",
            "model": ErrorResponse
        }
    }
)
async def delete_user(
    request: Request,
    user_id: str,
    admin: CurrentUser = Depends(require_admin),
    user_repo: UserRepository = Depends(get_user_repository),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    client_ip: str = Depends(get_client_ip),
    user_agent: Optional[str] = Depends(get_user_agent)
):
    """
    Delete user (soft delete).

    Args:
        request: HTTP request
        user_id: User UUID string
        admin: Admin user (from dependency)
        user_repo: User repository
        audit_repo: Audit repository
        client_ip: Client IP address
        user_agent: Client user agent

    Raises:
        HTTPException: If deletion fails
    """
    try:
        # Validate UUID
        try:
            user_uuid = UUID(user_id)
        except ValueError:
            logger.warning("invalid_user_id_format", user_id=user_id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid user ID format"
            )

        logger.info(
            "user_delete_attempt",
            admin_id=str(admin.id),
            admin_username=admin.username,
            user_id=user_id
        )

        # Get user before deletion for audit log
        user = await user_repo.get_user_by_id(user_uuid)
        if not user:
            logger.warning(
                "user_not_found",
                user_id=user_id,
                admin_username=admin.username
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Delete user (soft delete)
        deleted = await user_repo.delete_user(user_uuid)

        if not deleted:
            logger.warning(
                "user_not_found",
                user_id=user_id,
                admin_username=admin.username
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Log audit event
        await audit_repo.create_audit_log(
            user_id=admin.id,
            action=AuditAction.USER_DELETE.value,
            resource_type=ResourceType.USER.value,
            resource_id=user_id,
            details={
                "username": user.username,
                "deleted_by": admin.username
            },
            ip_address=client_ip,
            user_agent=user_agent,
            status_code=status.HTTP_204_NO_CONTENT
        )

        logger.info(
            "user_deleted",
            user_id=user_id,
            username=user.username,
            deleted_by=admin.username
        )

        return None

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "user_delete_error",
            error=str(e),
            user_id=user_id,
            admin_username=admin.username
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )


# ============================================================================
# ROUTER EXPORTS
# ============================================================================

# Export routers for main application
__all__ = ["auth_router", "admin_router"]