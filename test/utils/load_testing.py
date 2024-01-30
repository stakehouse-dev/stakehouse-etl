import os
from locust import HttpUser, task, between

class leaderBoard(HttpUser):
    wait_time = between(0.5, 2.5)

    @task
    def hello_world(self):
        self.client.get('/leaderboard', headers={"x-api-key": os.environ.get("API_KEY")})