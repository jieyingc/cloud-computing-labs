from locust import HttpUser, task, between

class IntegralUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def compute_integral(self):
        self.client.get("/numericalintegralservice/0/3.14159")

