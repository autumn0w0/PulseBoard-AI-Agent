import threading

class ThreadUserProjectStorage:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    # Set the full user data (from MongoDB)
    def set_user_data(self, user_data: dict):
        self._instance.user_data = user_data

    # Get the full user data
    def get_user_data(self) -> dict:
        return getattr(self._instance, "user_data", None)

    # Get all projects for the user
    def get_projects(self) -> list:
        user_data = self.get_user_data()
        if user_data and "projects" in user_data:
            return user_data["projects"]
        return []

    # Get a specific project by project_id
    def get_project(self, project_id: str) -> dict:
        for project in self.get_projects():
            if project["project_id"] == project_id:
                return project
        return None

    # Get MongoDB collections for a project
    def get_project_mongodb_collections(self, project_id: str) -> dict:
        project = self.get_project(project_id)
        if project and "mongodb" in project:
            return project["mongodb"].get("collections", {})
        return {}

    # Get Weaviate collections for a project
    def get_project_weaviate_collections(self, project_id: str) -> dict:
        project = self.get_project(project_id)
        if project and "weaviate" in project:
            return project["weaviate"].get("collections", {})
        return {}
