import numpy as np
import pandas as pd

def generate_requests():
    ID = pd.read_csv('businessID.txt', encoding='UTF-8').iloc[:, 0].tolist()
    simulation = []
    for i in range(500000):
        x1 = np.random.choice(ID)
        x2 = np.random.uniform(35.98, 36.31)
        x3 = np.random.uniform(-115.65, -115.04)
        simulation.append([x1, x2, x3])
    df = pd.DataFrame(simulation)
    df.columns = ['user_id', 'latitude', 'longitude']
    df.to_csv('simulated_requests.csv', header=False)

if __name__ == '__main__':
    generate_requests()