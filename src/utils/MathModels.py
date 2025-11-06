"""
Module: MathModels
This module provides mathematical models and functions for calculating energy 
consumption and power usage. It includes implementations for linear models and 
other possible alternatives such as cubic or minmax models.

Polynomial model function:
    coefficients is a list of coefficients for the polynomial model.
    The model will be of the form:
    y = a_n * x^n + a_(n-1) * x^(n-1) + ... + a_1 * x + a_0
    where n is the degree of the polynomial and a_i are the coefficients.

No file I/O is performed in this module. Exception handling is not required for the polynomial computations.
"""

from typing import List, Callable

def polynomial_model(coefficients: List[float]) -> Callable[[float], float]:
    """
    Create a polynomial model function based on provided coefficients.

    :param coefficients: List of coefficients [a_n, ..., a_0].
    :return: A function that calculates y for a given x using the polynomial.
    """
    def model(x: float) -> float:
        result = 0
        for i, coeff in enumerate(coefficients):
            result += coeff * (x ** (len(coefficients) - 1 - i))
        return result
    return model

linear_model: Callable[[float, float], Callable[[float], float]] = lambda a, b: polynomial_model([a, b])
quadratic_model: Callable[[float, float, float], Callable[[float], float]] = lambda a, b, c: polynomial_model([a, b, c])
cubic_model: Callable[[float, float, float, float], Callable[[float], float]] = lambda a, b, c, d: polynomial_model([a, b, c, d])

def min_max_linear_power_model(min: float, max: float) -> Callable[[float], float]:
    """
    Calculate a linear power model based on minimum and maximum watt values.

    :param min: Minimum watts.
    :param max: Maximum watts.
    :return: A linear model function reflecting the power range.
    """
    return linear_model((max - min) / 100, 0)

def baseline_linear_power_model(tdp_per_core: float) -> Callable[[float], float]:
    """
    Calculate a baseline linear power model using TDP per core.

    :param tdp_per_core: Thermal design power per core.
    :return: A linear model function scaled by the TDP.
    """
    return linear_model(tdp_per_core / 100, 0)

def fitted_linear_power_model(coefficient, intercept) -> Callable[[float], float]:
    """
    Calculate the fitted linear power model using the configured values.

    :param coefficient: the estimated coefficient.
    :param intercept: the estimated intercept.
    :return the fitted linear function reflecting the power range.
    """
    return linear_model(coefficient, 0)
