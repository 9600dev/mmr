import numpy as np
import matplotlib.pyplot as plt
import numpy.polynomial.hermite as herm
import math
# import sdeint as sde
from typing import List, Callable, Optional
from scipy.constants import pi
from trader.common.distributions import Distribution
import pandas as pd

class QuantumHarmonic(Distribution):
    def __init__(self,
                 name: str,
                 csv_file: str,
                 parameters: List[float],
                 data_column_apply: Optional[Callable[[pd.DataFrame, str], pd.Series]] = None,
                 cache_size: int = 365 * 20):
        super().__init__(name=name, cache_size=cache_size)
        self.csv_file = csv_file
        self.parameters = parameters
        self.data: List[float] = []
        self.cache_index: int = 0
        self.cache: List[float] = []
        self.pdf: List[float] = []
        self.init()

    def init(self):
        self.data = pd.read_csv(self.csv_file)['close'].tolist()
        self.populate_cache()

    def populate_cache(self):
        self.cache_index = 0
        self.cache = self.fit_qho(self.data[0], self.parameters, 1, self.data, 100)
        self.cache_size = len(self.cache)

    def sample(self) -> float:
        self.cache_index = self.cache_index + 1
        if self.cache_index >= self.cache_size:
            self.populate_cache()

        return self.cache[self.cache_index] / 100.0

    def qho_fp(self, x_range, t, C, mw, h_bar=1, n=5):
        # This is taken from original paper, "Modeling stock return distributions with a quantum harmonic oscillator"
        # Appears that t is the length of increments
        pdf = []
        for i in range(len(x_range)):
            x = x_range[i]
            p = 0
            for i in range(n):
            # En = n*h_bar*w
            # p += (An / np.sqrt((s**n)*np.factorial(n))) * np.sqrt((m*w)/(pi*h_bar))*np.exp(-En*t) * \
            # hermval(np.sqrt(m*w/h_bar)*x) * np.exp(-m*w*x**2/h_bar)
            # Using the simplified fit formula from the paper
                p_prime = C[i] * herm.hermval((mw/h_bar)**0.5 * x, i) * np.exp(-mw*x**2/(h_bar))
                p += p_prime.real

            if math.isinf(p) or math.isnan(p):
                return pdf[0:i]
            else:
                pdf.append(p)

        self.pdf = pdf
        return pdf

    def fit_qho(self, X0, parameters, t, data, N):
        min_val = min(data)
        max_val = max(data)
        x_range = np.linspace(min_val, max_val, num=200)
        qho = self.qho_fp(x_range, t, parameters[0:5], parameters[5])
        pdf = [1000 * i for i in qho]
        if len(qho) < len(x_range):
            return None

        else:
            model = [X0]
            sampling_vector = []
            for i in range(len(pdf)):
                for j in range(int(pdf[i])):
                    sampling_vector.append(x_range[i])

            for i in range(len(data)):
                model.append(np.random.choice(sampling_vector))

            return model

# def gbm(S0, mu, sigma, N):
#     """Geometric Brownian motion trajectory generator"""
#     t = range(N)
#     S = [S0]
#     for i in range(1, N):
#         W = np.random.normal()
#         d = (mu - 0.5 * sigma**2) * t[i]
#         diffusion = sigma * W
#         S_temp = S0 * np.exp(d + diffusion)

#         if math.isinf(S_temp) or math.isnan(S_temp):
#             return S[0:i]
#         else:
#             S.append(S_temp)
#     return S


# def gbm_fp(x_range, t, mu, sigma):
#     """Fokker-Planck equation for gbm"""

#     #PROBLEM HERE IS THAT THIS IS ONLY DEFINED FOR POSITIVE VVALUES
#     muhat = mu - 0.5*sigma**2
#     x0 = 0.1
#     pdf = [0]*len(x_range)
#     #populate pdf distrbiution
#     for i in range(len(x_range)):
#         x = x_range[i]
#         pdf[i] = (1/(sigma *x *np.sqrt(2*pi*t))) * np.exp(-(np.log(x)- \
#             np.log(x0)-muhat*t)**2/(2*t*sigma**2))
#         if math.isinf(pdf[i]) or math.isnan(pdf[i]):
#             return pdf[0:i]

#     return pdf


# def gbm_mod(X0, sigma_1, sigma_2, alpha, mu_step, N):
#     """modified GBM with Ornstein-Uhlenbeck term"""
#     #mu_step is the number of previous steps to include in the mean
#     t = np.linspace(0,1,N+1)
#     mus= [0]
#     X = [X0]
#     mus[0] = X0
#     for i in range(N-1):
#         if i < mu_step:
#             mu = np.average(X[0:i+1])
#         else:
#             mu = np.average(X[int(i-mu_step):int(i+1)])

#         mus.append(mu)

#         B = np.random.normal()

#         Y = X0 * np.exp(alpha*mu*i - 0.5*i*sigma_1**2 + sigma_1 * B)

#         #Use the sde library to complete integrals in second part of equation
#         increments = 1000
#         tspan = np.linspace(0, i, increments)

#         def f(x, t):
#             return (-alpha-sigma_1*sigma_2)/(X0 * np.exp((alpha*mu - 0.5*sigma_1**2)*t + sigma_1 * B))

#         def g(x,t):
#             return sigma_2/(X0 * np.exp((alpha*mu - 0.5*sigma_1**2)*t + sigma_1 * B))

#         sde_int = sde.itoint(f,g, X0, tspan)

#         #NEED TO ADD QUIT CONDITION IF INFINITY
#         x = Y * (X0 + np.sum(sde_int))
#         if math.isinf(x) or math.isnan(x):
#             return X[0:i]
#         else:
#             print(x)
#             X.append(x)

#     return X


# #def gbm_mod_fp():


# def quantum_harmonic_oscillator(n, xs, m=1, w=1, h_bar=1):
#     """Will return the quantum harmonic oscillator wavefunction for energy level n. NOT THE TRAJECTORY"""
#     xmin = min(xs)
#     xmax= max(xs)
#     psi = []
#     # coefficients for Hermite series, all 0s except the n-th term
#     herm_coeff = [0]*n
#     herm_coeff[-1] =1

#     for x in xs:
#         psi.append(math.exp(-m*w*x**2/(2*h_bar)) * herm.hermval((m*w/h_bar)**0.5 * x, herm_coeff)[1])
#     # normalization factor for the wavefunction:
#     psi = np.multiply(psi, 1 / (math.pow(2, n) * math.factorial(n))**0.5 * (m*w/(pi*h_bar))**0.25)

#     return xs, psi

    # def fit_gbm_mod(self, X0, parameters, N):
    #     alpha = parameters[0]
    #     sigma_1 = parameters[1]
    #     sigma_2 = parameters[2]
    #     mu_step = parameters[3]

    #     model = gbm_mod(X0, sigma_1, sigma_2, alpha, mu_step, N)
    #     print(model)
    #     print(len(model))
    #     if len(model) < N:
    #         return None

    #     else:
    #         return model


    # def fit_gbm(X0, parameters, N):
    #     mu = parameters[0]
    #     sigma = parameters[1]
    #     model = gbm(X0, mu, sigma, N)

    #     if len(model) < N:
    #         return None

    #     else:
    #         return model

