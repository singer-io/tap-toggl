from base import TogglBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class TogglAllFieldsTest(AllFieldsTest, TogglBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = {
        "projects": {
            "user_id", "project_id", "labor_cost", "project_name", "creator",
            "visibility", "archieved", "labor_cost_last_updated", "gid",
            "updated_at", "group_id", "deleted_at", "active_tasks", "manager",
        },
        "tags": {
            "permissions", "integration_ext_type", "integration_ext_id",
            "deleted_at", "integration_provider",
        },
        "workspaces": {
            "status", "organization_name", "user_status", "created_at",
            "role_id", "role_code", "active_users", "updated_at", "profile",
            "deleted_at", "total_users", "role_name",
        },
        "workspace_users": {
            "user_id",
            "workspace_id"
        },
        "clients": {
            "permissions", "integration_ext_type", "integration_ext_id",
            "external_reference", "notes", "integration_provider",
        },
        "users": {
            "timezone",
            "role",
            "2fa_enabled"
        },
    }

    @staticmethod
    def name():
        return "tap_tester_toggl_all_fields_test"

    def streams_to_test(self):
        # Due to test data not present excluding streams
        streams_to_exclude = {
            'groups',
            'tasks',
            'time_entries'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
