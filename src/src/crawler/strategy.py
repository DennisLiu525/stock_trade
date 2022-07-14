import math


def kali_eq(p: float, b: float, c: float):
    up = p*(b+c) - c
    down = b*c
    return up/down


if __name__ == "__main__":
    p = float(input("probability: "))
    b = float(input("win: "))
    c = float(input("lose: "))
    print(kali_eq(p, b, c))
