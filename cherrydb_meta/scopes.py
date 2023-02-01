"""Scopes (permissions) and role management."""
from dataclasses import dataclass
from cherrydb_meta.enums import ScopeType, NamespaceGroup
from cherrydb_meta.models import User, Namespace


@dataclass
class ScopeManager:
    """Common scopes/permissions queries for a user."""

    user: User

    def __post_init__(self):
        # Cache and aggregate scopes, which are eagerly loaded.
        user_group_scopes = {
            (scope.scope, scope.namespace_group)
            for scope in self.user.scopes
            if scope.namespace_id is None and scope.namespace_group is not None
        }
        user_namespace_scopes = {
            (scope.scope, scope.namespace_id)
            for scope in self.user.scopes
            if scope.namespace_id is not None and scope.namespace_group is None
        }
        user_global_scopes = {
            scope.scope
            for scope in self.user.scopes
            if scope.namespace_id is None
            and (
                scope.namespace_group is None
                or scope.namespace_group == NamespaceGroup.ALL
            )
        }
        group_group_scopes = {
            (scope.scope, scope.namespace_group)
            for group in self.user.groups
            for scope in group.group.scopes
            if scope.namespace_id is None and scope.namespace_group is not None
        }
        group_namespace_scopes = {
            (scope.scope, scope.namespace_id)
            for group in self.user.groups
            for scope in group.group.scopes
            if scope.namespace_id is not None and scope.namespace_group is None
        }
        group_global_scopes = {
            scope.scope
            for group in self.user.groups
            for scope in group.group.scopes
            if scope.namespace_id is None
            and (
                scope.namespace_group is None
                or scope.namespace_group == NamespaceGroup.ALL
            )
        }
        self._namespace_scopes = user_namespace_scopes | group_namespace_scopes
        self._namespace_group_scopes = user_group_scopes | group_group_scopes
        self._global_scopes = user_global_scopes | group_global_scopes

    def can_read_localities(self):
        return self._has_global_scope(ScopeType.LOCALITY_READ)

    def can_write_localities(self):
        return self._has_global_scope(ScopeType.LOCALITY_WRITE)

    def can_read_meta(self):
        return self._has_global_scope(ScopeType.META_READ)

    def can_write_meta(self):
        return self._has_global_scope(ScopeType.META_WRITE)

    def can_create_namespace(self):
        return self._has_global_scope(ScopeType.NAMESPACE_CREATE)

    def can_read_in_namespace(self, namespace: Namespace):
        return self._has_namespace_scope(ScopeType.NAMESPACE_READ, namespace)

    def can_write_in_namespace(self, namespace: Namespace):
        return self._has_namespace_scope(ScopeType.NAMESPACE_WRITE, namespace)

    def can_write_derived_in_namespace(self, namespace: Namespace):
        return self._has_namespace_scope(
            ScopeType.NAMESPACE_WRITE, namespace
        ) or self._has_namespace_scope(ScopeType.NAMESPACE_WRITE_DERIVED, namespace)

    def _has_global_scope(self, scope: ScopeType) -> bool:
        """Does the user have the global scope `scope`?"""
        return ScopeType.ALL in self._global_scopes or scope in self._global_scopes

    def _has_namespace_scope(self, scope: ScopeType, namespace: Namespace) -> bool:
        """Does the user have `scope` in `namespace`?"""
        candidates = {
            (scope, namespace.namespace_id),
            (ScopeType.ALL, namespace.namespace_id),
        }
        group = NamespaceGroup.PUBLIC if namespace.public else NamespaceGroup.PRIVATE
        return bool(
            self._namespace_scopes & candidates
        ) or self._has_namespace_group_scope(scope, group)

    def _has_namespace_group_scope(
        self, scope: ScopeType, group: NamespaceGroup
    ) -> bool:
        """Does the user have `scope` in `group`?"""
        candidates = {
            (scope, group),
            (ScopeType.ALL, group),
            (scope, NamespaceGroup.ALL),
            (ScopeType.ALL, NamespaceGroup.ALL),
        }
        return bool(candidates & self._namespace_group_scopes)
